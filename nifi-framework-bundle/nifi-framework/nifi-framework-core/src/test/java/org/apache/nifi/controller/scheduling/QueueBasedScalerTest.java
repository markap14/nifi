/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueueBasedScalerTest {

    private QueueBasedScaler scaler;
    private Connectable validationConnectable;

    @BeforeEach
    void setUp() {
        scaler = new QueueBasedScaler(mock(FlowFileEventRepository.class));
        validationConnectable = mock(Connectable.class);
        when(validationConnectable.getName()).thenReturn("TestProcessor");
    }

    @Test
    void testShouldScaleDownWhenAtMinimum() {
        assertFalse(scaler.shouldScaleDown(1, 0.0, 100, true));
    }

    @Test
    void testShouldScaleDownWhenOutboundPressureHigh() {
        assertTrue(scaler.shouldScaleDown(3, 0.85, 100, true));
    }

    @Test
    void testShouldScaleDownWhenInboundEmpty() {
        assertTrue(scaler.shouldScaleDown(3, 0.2, 0, true));
    }

    @Test
    void testShouldNotScaleDownWhenInboundHasWorkAndNoBackpressure() {
        assertFalse(scaler.shouldScaleDown(3, 0.2, 50, true));
    }

    @Test
    void testShouldNotScaleDownSourceProcessorDueToEmptyInbound() {
        assertFalse(scaler.shouldScaleDown(3, 0.2, 0, false));
    }

    @Test
    void testShouldScaleDownSourceProcessorOnOutboundBackpressure() {
        assertTrue(scaler.shouldScaleDown(3, 0.85, 0, false));
    }

    @Test
    void testShouldScaleUpWhenInboundHasWork() {
        final ScalingState state = new ScalingState(12);
        assertTrue(scaler.shouldScaleUp(1, state, 0.2, 100, true));
    }

    @Test
    void testShouldNotScaleUpAtMaxConcurrency() {
        final ScalingState state = new ScalingState(4);
        assertFalse(scaler.shouldScaleUp(4, state, 0.0, 100, true));
    }

    @Test
    void testShouldNotScaleUpDuringCooldown() {
        final ScalingState state = new ScalingState(12);
        state.enterCooldown();
        assertFalse(scaler.shouldScaleUp(1, state, 0.0, 100, true));
    }

    @Test
    void testShouldNotScaleUpDuringPendingValidation() {
        final ScalingState state = new ScalingState(12);
        state.setPendingValidation(true);
        assertFalse(scaler.shouldScaleUp(1, state, 0.0, 100, true));
    }

    @Test
    void testShouldNotScaleUpWhenOutboundPressureHigh() {
        final ScalingState state = new ScalingState(12);
        assertFalse(scaler.shouldScaleUp(1, state, 0.85, 100, true));
    }

    @Test
    void testShouldScaleUpSourceProcessor() {
        final ScalingState state = new ScalingState(12);
        assertTrue(scaler.shouldScaleUp(1, state, 0.0, 0, false));
    }

    @Test
    void testShouldScaleUpWhenInboundHasWorkAndNoBackpressure() {
        final ScalingState state = new ScalingState(12);
        assertTrue(scaler.shouldScaleUp(1, state, 0.3, 100, true));
    }

    @Test
    void testShouldNotScaleUpWhenInboundEmpty() {
        final ScalingState state = new ScalingState(12);
        assertFalse(scaler.shouldScaleUp(1, state, 0.0, 0, true));
    }

    @Test
    void testValidateScaleUpRollsBackWhenThroughputDidNotImprove() {
        final ScalingState state = new ScalingState(12);
        state.getTargetConcurrency().set(3);
        state.setPreScaleUpFlowFilesRate(100);
        state.setPreScaleUpBytesRate(5000);
        state.setPendingValidation(true);

        scaler.validateScaleUp(validationConnectable, state, 90, 4000);

        assertEquals(2, state.getTargetConcurrency().get());
        assertFalse(state.isPendingValidation());
        assertTrue(state.isInCooldown());
    }

    @Test
    void testValidateScaleUpNeverDecrementsBelow1() {
        final ScalingState state = new ScalingState(12);
        state.getTargetConcurrency().set(1);
        state.setPreScaleUpFlowFilesRate(100);
        state.setPreScaleUpBytesRate(5000);
        state.setPendingValidation(true);

        scaler.validateScaleUp(validationConnectable, state, 50, 2000);

        assertEquals(1, state.getTargetConcurrency().get());
        assertFalse(state.isPendingValidation());
        assertTrue(state.isInCooldown());
    }

    @Test
    void testValidateScaleUpKeepsTargetWhenFlowFilesImproved() {
        final ScalingState state = new ScalingState(12);
        state.getTargetConcurrency().set(3);
        state.setPreScaleUpFlowFilesRate(100);
        state.setPreScaleUpBytesRate(5000);
        state.setPendingValidation(true);

        scaler.validateScaleUp(validationConnectable, state, 150, 4000);

        assertEquals(3, state.getTargetConcurrency().get());
        assertFalse(state.isPendingValidation());
        assertFalse(state.isInCooldown());
    }

    @Test
    void testValidateScaleUpKeepsTargetWhenBytesImproved() {
        final ScalingState state = new ScalingState(12);
        state.getTargetConcurrency().set(3);
        state.setPreScaleUpFlowFilesRate(100);
        state.setPreScaleUpBytesRate(5000);
        state.setPendingValidation(true);

        scaler.validateScaleUp(validationConnectable, state, 90, 6000);

        assertEquals(3, state.getTargetConcurrency().get());
        assertFalse(state.isPendingValidation());
        assertFalse(state.isInCooldown());
    }

    @Test
    void testIdleHoldBackoffStartsTimerOnFirstHold() {
        final ScalingState state = new ScalingState(4);
        state.getTargetConcurrency().set(4);
        final Connectable connectable = createConnectableWithInboundPressure(0.1);

        final ScalingRecommendation recommendation = scaler.evaluate(connectable, state);

        assertEquals(ScalingRecommendation.HOLD, recommendation);
        assertTrue(state.getIdleHoldSince() > 0);
    }

    @Test
    void testIdleHoldBackoffScalesDownAfterDuration() {
        final ScalingState state = new ScalingState(4);
        state.getTargetConcurrency().set(4);
        state.setIdleHoldSince(System.currentTimeMillis() - 3100);
        final Connectable connectable = createConnectableWithInboundPressure(0.1);

        final ScalingRecommendation recommendation = scaler.evaluate(connectable, state);

        assertEquals(ScalingRecommendation.SCALE_DOWN, recommendation);
    }

    @Test
    void testIdleHoldBackoffResetsWhenInboundPressureHigh() {
        final ScalingState state = new ScalingState(4);
        state.getTargetConcurrency().set(4);
        state.setIdleHoldSince(System.currentTimeMillis() - 2000);
        final Connectable connectable = createConnectableWithInboundPressure(0.5);

        final ScalingRecommendation recommendation = scaler.evaluate(connectable, state);

        assertEquals(ScalingRecommendation.HOLD, recommendation);
        assertEquals(0, state.getIdleHoldSince());
    }

    @Test
    void testIdleHoldBackoffDoesNotApplyAtConcurrencyOne() {
        final ScalingState state = new ScalingState(1);
        state.getTargetConcurrency().set(1);
        state.setIdleHoldSince(System.currentTimeMillis() - 5000);
        final Connectable connectable = createConnectableWithInboundPressure(0.1);

        final ScalingRecommendation recommendation = scaler.evaluate(connectable, state);

        assertEquals(ScalingRecommendation.HOLD, recommendation);
    }

    private Connectable createConnectableWithInboundPressure(final double inboundPressure) {
        final Connectable connectable = mock(Connectable.class);
        when(connectable.getIdentifier()).thenReturn("test-id");
        when(connectable.hasIncomingConnection()).thenReturn(true);

        final FlowFileQueue inboundQueue = mock(FlowFileQueue.class);
        when(inboundQueue.getBackPressureRatio()).thenReturn(inboundPressure);
        final int queuedCount = inboundPressure > 0 ? 100 : 0;
        when(inboundQueue.size()).thenReturn(new QueueSize(queuedCount, 0L));

        final Connection inboundConnection = mock(Connection.class);
        when(inboundConnection.getFlowFileQueue()).thenReturn(inboundQueue);
        final Connectable sourceConnectable = mock(Connectable.class);
        when(inboundConnection.getSource()).thenReturn(sourceConnectable);

        when(connectable.getIncomingConnections()).thenReturn(List.of(inboundConnection));
        when(connectable.getConnections()).thenReturn(Collections.emptySet());
        when(connectable.isTriggerWhenAnyDestinationAvailable()).thenReturn(false);

        return connectable;
    }
}
