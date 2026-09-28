// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.eventbus;

import com.gitlab.seabeam.controlplane.schematic.services.eventbus.model.types.sys.healthreport.Healthreport.InstanceStatus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * checkHealth() must tolerate transient Kafka probe failures — a rebalance or a
 * recovering broker is not a wedged controller — and only report UNHEALTHY once
 * the failures have persisted for longer than a real post-restart rebalance.
 *
 * The streak length is asserted as wall-clock tolerance rather than as a raw
 * count, because the count is only meaningful against the health reporter's
 * cadence, and it was a count chosen against the k8s probe default (3 = ~30s)
 * that let the 2026-09-13 crash loop happen.
 */
class HealthStreakTest {

	/**
	 * Longest partition-assignment wait measured on the live cluster during the
	 * 2026-09-13 incident, across ~25 subscribers reconnecting at once. The
	 * tolerance has to clear this or eventbus kills itself mid-rebalance.
	 */
	private static final long OBSERVED_WORST_REBALANCE_MS = 135_786;

	private static EventBusHandler handler() {
		return new EventBusHandler(null);
	}

	/** Feeds n consecutive failures and returns the status reported by the last one. */
	private static InstanceStatus afterFailures(EventBusHandler h, int n) {
		InstanceStatus last = null;
		for (int i = 0; i < n; i++) {
			last = h.recordProbe(false, "timeout");
		}
		return last;
	}

	@Test
	void singleFailureStaysHealthy() {
		EventBusHandler h = handler();
		assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(false, "timeout"));
	}

	@Test
	void failuresJustShortOfTheThresholdStayHealthy() {
		EventBusHandler h = handler();
		assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY,
				afterFailures(h, EventBusHandler.UNHEALTHY_STREAK_THRESHOLD - 1));
	}

	@Test
	void thresholdFailuresReportUnhealthy() {
		EventBusHandler h = handler();
		assertEquals(InstanceStatus.INSTANCE_STATUS_UNHEALTHY,
				afterFailures(h, EventBusHandler.UNHEALTHY_STREAK_THRESHOLD));
		// Stays unhealthy while the failures continue — a wedged controller is
		// still eventually restarted, which is what this check exists for.
		assertEquals(InstanceStatus.INSTANCE_STATUS_UNHEALTHY, h.recordProbe(false, "timeout"));
	}

	@Test
	void successResetsTheStreak() {
		EventBusHandler h = handler();
		afterFailures(h, EventBusHandler.UNHEALTHY_STREAK_THRESHOLD - 1);
		assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, h.recordProbe(true, null));
		// The next run has to start over from one.
		assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY,
				afterFailures(h, EventBusHandler.UNHEALTHY_STREAK_THRESHOLD - 1));
		assertEquals(InstanceStatus.INSTANCE_STATUS_UNHEALTHY, h.recordProbe(false, "timeout"));
	}

	@Test
	void uninitializedHandlerReportsHealthy() {
		assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY, handler().checkHealth());
	}

	/**
	 * The regression test for the crash loop itself: a rebalance as long as the
	 * worst one actually measured must not be enough to report UNHEALTHY, since
	 * that report is what flips the shared health port to NOT_SERVING and gets
	 * the pod killed mid-rebalance.
	 */
	@Test
	void theWorstObservedRebalanceDoesNotReportUnhealthy() {
		int probesDuringRebalance =
				(int) (OBSERVED_WORST_REBALANCE_MS / EventBusHandler.HEALTH_REPORT_INTERVAL_MS);
		EventBusHandler h = handler();
		assertEquals(InstanceStatus.INSTANCE_STATUS_HEALTHY,
				afterFailures(h, probesDuringRebalance),
				"a " + OBSERVED_WORST_REBALANCE_MS + "ms rebalance (" + probesDuringRebalance
						+ " failed probes) must not vote the pod dead");
	}

	/**
	 * And the tolerance must keep real margin over that worst case rather than
	 * merely clearing it, because the pod is killed the moment it does not.
	 */
	@Test
	void toleranceClearsTheObservedWorstCaseWithMargin() {
		long toleranceMs =
				EventBusHandler.UNHEALTHY_STREAK_THRESHOLD * EventBusHandler.HEALTH_REPORT_INTERVAL_MS;
		assertTrue(toleranceMs >= OBSERVED_WORST_REBALANCE_MS * 1.5,
				"health tolerance is " + toleranceMs + "ms; needs >= 1.5x the observed worst "
						+ "rebalance of " + OBSERVED_WORST_REBALANCE_MS + "ms");
	}

	/**
	 * The other half of the trade: a genuinely wedged KRaft controller must
	 * still get the pod restarted, and it must happen inside the launcher's 5m
	 * HEALTH_TIMEOUT or the launcher kills the pod first with a less useful
	 * signal. This is the upper bound on the threshold.
	 */
	@Test
	void wedgedControllerIsStillRestartedInsideTheLauncherTimeout() {
		long toleranceMs =
				EventBusHandler.UNHEALTHY_STREAK_THRESHOLD * EventBusHandler.HEALTH_REPORT_INTERVAL_MS;
		assertTrue(toleranceMs < 300_000,
				"health tolerance is " + toleranceMs + "ms; a wedged controller must be reported "
						+ "UNHEALTHY inside the launcher's 300000ms HEALTH_TIMEOUT");
	}
}
