// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for the 2026-09-13 eventbus crash loop.
 *
 * The partition-assignment budget was {@code 12 attempts x 5000ms = 60000ms}
 * and {@code max.poll.interval.ms} was also {@code 60000ms}. A session waiting
 * for partitions is blocked inside {@code poll()}, so those two deadlines are
 * the same stretch of wall clock measured by two different parties — and when
 * they are equal, at t+60s every member of a still-rebalancing group
 * simultaneously:
 *
 * <ul>
 *   <li>gives up its own assignment wait, ending the session and leaving the
 *       group — which starts another rebalance, and</li>
 *   <li>is evicted by the coordinator for exceeding max.poll.interval.ms —
 *       which starts another rebalance.</li>
 * </ul>
 *
 * A rebalance cannot complete until every member rejoins, so a group that
 * takes longer than the budget to converge can never converge at all: the
 * deadline keeps firing and re-dirtying the group. Observed live as 16 members
 * of one group abandoning within the same second, 20 poll stalls, and 7 pod
 * restarts into CrashLoopBackOff.
 *
 * See {@code decisions/2026-09-13-assignment-budget-equals-poll-interval.md}.
 */
class SubscriberTimeoutBudgetTest {

	/**
	 * Longest partition-assignment wait measured on the live cluster during the
	 * incident (a scottyagent AgentCycleBus session, 135786ms), with ~25
	 * subscribers reconnecting inside one second after a restart.
	 */
	private static final long OBSERVED_WORST_REBALANCE_MS = 135_786;

	private static long assignmentBudgetMs() {
		return KafkaSubscriber.ASSIGNMENT_MAX_ATTEMPTS * KafkaSubscriber.ASSIGNMENT_POLL_MS;
	}

	/** The invariant, on the values actually shipped. */
	@Test
	void assignmentBudgetIsStrictlyUnderTheEvictionCeiling() {
		assertTrue(assignmentBudgetMs() < KafkaSubscriber.MAX_POLL_INTERVAL_MS,
				"assignment budget " + assignmentBudgetMs() + "ms must be < max.poll.interval.ms "
						+ KafkaSubscriber.MAX_POLL_INTERVAL_MS + "ms");
	}

	/**
	 * Strictly-less is not enough on its own: the gap has to be wide enough
	 * that a session which burns its entire budget still has room to get back
	 * into poll() before the coordinator drops it.
	 */
	@Test
	void theGapLeavesRoomToRecoverAfterExhaustingTheBudget() {
		long gapMs = KafkaSubscriber.MAX_POLL_INTERVAL_MS - assignmentBudgetMs();
		assertTrue(gapMs >= KafkaSubscriber.ASSIGNMENT_POLL_MS * 2,
				"only " + gapMs + "ms between the assignment budget and eviction; a session that "
						+ "uses its whole budget would be evicted almost immediately after");
	}

	/** The exact production configuration must now be rejected at startup. */
	@Test
	void theConfigurationThatCrashLoopedProductionIsRejected() {
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> KafkaSubscriber.validateTimeoutBudgets(12, 5000L, 60000L));
		assertTrue(e.getMessage().contains("60000ms"), e.getMessage());
	}

	/** So must any budget that merely equals the ceiling, at any scale. */
	@Test
	void anEqualBudgetAndCeilingIsRejected() {
		assertThrows(IllegalStateException.class,
				() -> KafkaSubscriber.validateTimeoutBudgets(36, 5000L, 180000L));
		assertThrows(IllegalStateException.class,
				() -> KafkaSubscriber.validateTimeoutBudgets(1, 1000L, 1000L));
	}

	/** And a budget past the ceiling, which is worse still. */
	@Test
	void aBudgetOverTheCeilingIsRejected() {
		assertThrows(IllegalStateException.class,
				() -> KafkaSubscriber.validateTimeoutBudgets(60, 5000L, 60000L));
	}

	/** The shipped values pass their own guard. */
	@Test
	void theShippedConfigurationPassesTheGuard() {
		assertDoesNotThrow(() -> KafkaSubscriber.validateTimeoutBudgets(
				KafkaSubscriber.ASSIGNMENT_MAX_ATTEMPTS,
				KafkaSubscriber.ASSIGNMENT_POLL_MS,
				KafkaSubscriber.MAX_POLL_INTERVAL_MS));
	}

	/**
	 * The budget must actually cover a real post-restart rebalance. This is the
	 * half the old 60000ms budget failed: it was under half the real duration,
	 * so the timeout fired on every single restart rather than exceptionally.
	 */
	@Test
	void assignmentBudgetCoversTheObservedWorstRebalance() {
		assertTrue(assignmentBudgetMs() > OBSERVED_WORST_REBALANCE_MS,
				"assignment budget " + assignmentBudgetMs() + "ms does not cover the observed "
						+ OBSERVED_WORST_REBALANCE_MS + "ms rebalance, so it would fire on a "
						+ "normal restart");
	}

	/**
	 * Raising the eviction ceiling must not have cost us the detection latency
	 * for a genuinely wedged consumer — the "group Stable, pod 1/1 Ready,
	 * nothing consumed" mode. That is the poll-stall watchdog's job now, and it
	 * still reports at the old 60s.
	 */
	@Test
	void wedgedConsumerDetectionIsUnchangedAtSixtySeconds() {
		assertEquals(60_000L, KafkaSubscriber.POLL_STALL_WARN_MS);
		assertTrue(KafkaSubscriber.POLL_STALL_WARN_MS < KafkaSubscriber.MAX_POLL_INTERVAL_MS,
				"a stall must be reported before the coordinator evicts, not after");
	}

	/**
	 * The mid-batch keep-alive has to stay comfortably inside the eviction
	 * ceiling, since it is what stops a slow gRPC client from getting the
	 * member dropped.
	 */
	@Test
	void processingBudgetStaysUnderTheEvictionCeiling() {
		assertTrue(KafkaSubscriber.PROCESSING_BUDGET_MS < KafkaSubscriber.MAX_POLL_INTERVAL_MS,
				"processing budget " + KafkaSubscriber.PROCESSING_BUDGET_MS
						+ "ms must leave slack before eviction at "
						+ KafkaSubscriber.MAX_POLL_INTERVAL_MS + "ms");
	}

	/**
	 * And it has to fire before the poll-stall watchdog would report the
	 * session, or every legitimately slow batch logs a spurious STALLED error.
	 * Deriving the budget from the eviction ceiling (now 300s) instead of the
	 * warn threshold puts it at 150s, i.e. 90s after the warning.
	 */
	@Test
	void theKeepAliveFiresBeforeAStallIsReported() {
		assertTrue(KafkaSubscriber.PROCESSING_BUDGET_MS < KafkaSubscriber.POLL_STALL_WARN_MS,
				"processing budget " + KafkaSubscriber.PROCESSING_BUDGET_MS
						+ "ms must be under the stall warning at "
						+ KafkaSubscriber.POLL_STALL_WARN_MS + "ms");
	}
}
