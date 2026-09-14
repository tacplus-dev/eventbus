// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.grpc.SubscribeHandler;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Caller;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.s2sm.internal.grpc.servers.services.eventbus.Admin;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.types.sys.errors.ProtoException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 2026-09-13 security audit, Critical: the Admin RPCs took a {@code Caller} and
 * never looked at it.
 *
 * <p>{@code Admin} is declared {@code auth_services: ["*"]}, so the Authorizer
 * admits every registered platform service, and every admin method then acted
 * on whatever group or topic the request named. Meanwhile the subscribe path
 * binds each caller's consumer group under its own SPIFFE subject
 * ({@code SubscribeHandler.boundConsumerGroup}) precisely so no other caller can
 * touch it — isolation Admin routed straight around: any service could delete
 * or rewind stratum's or mailbox's bound group, forcing a full backlog replay
 * or silently skipping everything published since.
 *
 * <p>These drive the real registered admin implementation
 * ({@code EventBusHandler.getAdminGrpcImpl()}) with the real {@code Caller} the
 * generated wrapper hands it. Kafka is deliberately absent: a call that clears
 * the caller check reaches the handler body and comes back
 * UNAVAILABLE("Kafka not yet initialized"), which is what distinguishes
 * "allowed through" from "denied" — the same seam
 * {@link TopicAuthorizationTest} uses.
 *
 * <p>{@link #theOwnersOwnGroupStillGetsThrough} is the guard against a vacuous
 * suite: if the gate ever started denying everything, the denial assertions
 * would still pass and that one would fail.
 */
class AdminCallerScopingTest {

    private static final String VICTIM_SUB = "spiffe://test/system/services/stratum";
    private static final String ATTACKER_SUB = "spiffe://test/system/services/tasks";

    /** The group id stratum's cold-start rebuild really uses (ColdStartRebuilder). */
    private static final String GROUP = "stratum-rebuild";

    /** What the victim's subscribe put in Kafka, and what ListConsumerGroups reports. */
    private static final String VICTIM_BOUND_GROUP = VICTIM_SUB + "|" + GROUP;

    private static Caller caller(String sub) {
        return new Caller("{\"sub\":\"" + sub + "\"}", "test-request-id");
    }

    private static Admin.AdminHandler admin() {
        // sm is only dereferenced by the lifecycle methods, none of which run
        // here; topicManager stays null, which is the "Kafka not yet
        // initialized" state the allowed path lands in.
        return new EventBusHandler(null).getAdminGrpcImpl();
    }

    private static Eventbus.ConsumerGroupRequest groupRequest(String groupId) {
        return Eventbus.ConsumerGroupRequest.newBuilder().setGroupId(groupId).build();
    }

    private static Eventbus.ResetConsumerGroupOffsetsRequest resetRequest(String groupId) {
        return Eventbus.ResetConsumerGroupOffsetsRequest.newBuilder()
                .setGroupId(groupId)
                .setTopic("services.messaging.messaging.MessageBus.Stream")
                .setToEarliest(true)
                .build();
    }

    // ---- the reported gap: naming another service's bound group ----

    @Test
    void deleteConsumerGroup_onAnotherServicesBoundGroup_isDenied() {
        ProtoException e = assertThrows(ProtoException.class,
                () -> admin().deleteConsumerGroup(caller(ATTACKER_SUB), groupRequest(VICTIM_BOUND_GROUP)));
        assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
    }

    @Test
    void resetConsumerGroupOffsets_onAnotherServicesBoundGroup_isDenied() {
        ProtoException e = assertThrows(ProtoException.class,
                () -> admin().resetConsumerGroupOffsets(caller(ATTACKER_SUB), resetRequest(VICTIM_BOUND_GROUP)));
        assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
    }

    @Test
    void anUnverifiedCaller_isDenied() {
        // The generated wrapper passes null when nothing verified a token.
        ProtoException e = assertThrows(ProtoException.class,
                () -> admin().deleteConsumerGroup(null, groupRequest(VICTIM_BOUND_GROUP)));
        assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
    }

    @Test
    void anEmptyGroupId_isDenied() {
        // "" is the no-group sentinel on the subscribe path; it names nothing
        // deletable, and must not fall through to Kafka as a wildcard.
        ProtoException e = assertThrows(ProtoException.class,
                () -> admin().deleteConsumerGroup(caller(ATTACKER_SUB), groupRequest("")));
        assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
    }

    // ---- the owner is still served ----

    @Test
    void theOwnersOwnGroupStillGetsThrough() {
        // Both spellings the owner can use: the plain name it passed to
        // Subscribe, and the bound id ListConsumerGroups reports back.
        for (String named : new String[]{GROUP, VICTIM_BOUND_GROUP}) {
            ProtoException e = assertThrows(ProtoException.class,
                    () -> admin().deleteConsumerGroup(caller(VICTIM_SUB), groupRequest(named)));
            assertEquals(ProtoException.UNAVAILABLE, e.getCode(),
                    "owner naming \"" + named + "\" should clear the caller check and reach Kafka");

            ProtoException r = assertThrows(ProtoException.class,
                    () -> admin().resetConsumerGroupOffsets(caller(VICTIM_SUB), resetRequest(named)));
            assertEquals(ProtoException.UNAVAILABLE, r.getCode(),
                    "owner naming \"" + named + "\" should clear the caller check and reach Kafka");
        }
    }

    // ---- the group the caller actually acts on ----

    @Test
    void thePlainGroupNameResolvesToTheCallersOwnGroup() throws ProtoException {
        assertEquals(VICTIM_BOUND_GROUP,
                EventBusHandler.adminOwnedGroup(caller(VICTIM_SUB), GROUP));

        // The attacker naming the same plain name reaches its OWN group, which
        // is the whole point of the binding scheme -- never the victim's.
        String attackerTarget = EventBusHandler.adminOwnedGroup(caller(ATTACKER_SUB), GROUP);
        assertEquals(ATTACKER_SUB + "|" + GROUP, attackerTarget);
        assertNotEquals(VICTIM_BOUND_GROUP, attackerTarget);
    }

    @Test
    void anIdBoundToAnotherSubjectIsNotRebound() {
        // Binding it again would produce "attacker|victim|group", a group that
        // exists for nobody; denying says what actually happened.
        assertEquals("", SubscribeHandler.ownedConsumerGroup(ATTACKER_SUB, VICTIM_BOUND_GROUP));
    }

    // ---- listConsumerGroups scoping ----

    @Test
    void ownershipExcludesEveryGroupButTheCallersOwn() {
        assertTrue(SubscribeHandler.ownsConsumerGroup(VICTIM_SUB, VICTIM_BOUND_GROUP));
        assertFalse(SubscribeHandler.ownsConsumerGroup(ATTACKER_SUB, VICTIM_BOUND_GROUP));
        // An anonymous subscribe's random group id and eventbus's own internal
        // groups carry no subject at all, so they belong to no caller.
        assertFalse(SubscribeHandler.ownsConsumerGroup(VICTIM_SUB, "eventbus-trace-dispatch"));
        assertFalse(SubscribeHandler.ownsConsumerGroup("", VICTIM_BOUND_GROUP));
        // A subject that is a prefix of another must not match across the
        // separator.
        assertFalse(SubscribeHandler.ownsConsumerGroup(VICTIM_SUB, VICTIM_SUB + "-shadow|" + GROUP));
    }

    // ---- getConsumerGroupLag ----
    //
    // ListConsumerGroups scoping stops enumeration, not a targeted guess: lag
    // answered for any group id a caller could name, handing back that group's
    // members, assigned topics and offsets.

    @Test
    void getConsumerGroupLag_onAnotherServicesBoundGroup_isDenied() {
        ProtoException e = assertThrows(ProtoException.class,
                () -> admin().getConsumerGroupLag(caller(ATTACKER_SUB), groupRequest(VICTIM_BOUND_GROUP)));
        assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
    }

    @Test
    void getConsumerGroupLag_forAnUnverifiedCallerOrEmptyGroup_isDenied() {
        ProtoException e = assertThrows(ProtoException.class,
                () -> admin().getConsumerGroupLag(null, groupRequest(VICTIM_BOUND_GROUP)));
        assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());

        ProtoException b = assertThrows(ProtoException.class,
                () -> admin().getConsumerGroupLag(caller(ATTACKER_SUB), groupRequest("")));
        assertEquals(ProtoException.PERMISSION_DENIED, b.getCode(), b.getMessage());
    }

    @Test
    void getConsumerGroupLag_onTheOwnersOwnGroup_stillGetsThrough() {
        // The positive half, for the same reason theOwnersOwnGroupStillGetsThrough
        // exists: a gate that denied everything would still pass the assertions
        // above. Both spellings the owner can use.
        for (String named : new String[]{GROUP, VICTIM_BOUND_GROUP}) {
            ProtoException e = assertThrows(ProtoException.class,
                    () -> admin().getConsumerGroupLag(caller(VICTIM_SUB), groupRequest(named)));
            assertEquals(ProtoException.UNAVAILABLE, e.getCode(),
                    "owner naming \"" + named + "\" should clear the caller check and reach Kafka");
        }
    }

    // ---- updateTopicConfig ----

    @Test
    void updateTopicConfig_isDeniedToEveryCaller() {
        // Retention shrinkage is DeleteTopic's data loss in slow motion, and
        // topic ownership is not derivable from the registry, so this is denied
        // outright rather than scoped. No caller in the fleet invokes it.
        Eventbus.UpdateTopicConfigRequest request = Eventbus.UpdateTopicConfigRequest.newBuilder()
                .setTopic("services.messaging.messaging.MessageBus.Stream")
                .setRetentionMs(1000)
                .build();
        for (String sub : new String[]{VICTIM_SUB, ATTACKER_SUB}) {
            ProtoException e = assertThrows(ProtoException.class,
                    () -> admin().updateTopicConfig(caller(sub), request));
            assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
        }
    }

    // ---- deleteTopic ----

    @Test
    void deleteTopic_isDeniedToEveryCaller() {
        Eventbus.DeleteTopicRequest request = Eventbus.DeleteTopicRequest.newBuilder()
                .setTopic("services.messaging.messaging.MessageBus.Stream").build();
        for (String sub : new String[]{VICTIM_SUB, ATTACKER_SUB}) {
            ProtoException e = assertThrows(ProtoException.class,
                    () -> admin().deleteTopic(caller(sub), request));
            assertEquals(ProtoException.PERMISSION_DENIED, e.getCode(), e.getMessage());
        }
    }
}
