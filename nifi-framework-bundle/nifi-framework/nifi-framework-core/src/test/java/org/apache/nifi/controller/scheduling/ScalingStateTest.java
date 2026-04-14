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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScalingStateTest {

    @Test
    void testInitialState() {
        final ScalingState state = new ScalingState(12);
        assertEquals(1, state.getTargetConcurrency().get());
        assertEquals(12, state.getMaxConcurrency());
        assertFalse(state.isPendingValidation());
        assertFalse(state.isInCooldown());
    }

    @Test
    void testEvaluationDueInitially() {
        final ScalingState state = new ScalingState(12);
        assertTrue(state.isEvaluationDue());
    }

    @Test
    void testEvaluationNotDueAfterRecent() {
        final ScalingState state = new ScalingState(12);
        state.setLastEvaluationTime(System.currentTimeMillis());
        assertFalse(state.isEvaluationDue());
    }

    @Test
    void testEvaluationDueAfterInterval() throws InterruptedException {
        final ScalingState state = new ScalingState(12);
        state.setLastEvaluationTime(System.currentTimeMillis() - 1100);
        assertTrue(state.isEvaluationDue());
    }

    @Test
    void testCooldownNotActiveInitially() {
        final ScalingState state = new ScalingState(12);
        assertFalse(state.isInCooldown());
    }

    @Test
    void testEnterCooldown() {
        final ScalingState state = new ScalingState(12);
        state.enterCooldown();
        assertTrue(state.isInCooldown());
    }

    @Test
    void testCooldownExpires() {
        final ScalingState state = new ScalingState(12);
        state.setCooldownExpiration(System.currentTimeMillis() - 1);
        assertFalse(state.isInCooldown());
    }

    @Test
    void testTargetConcurrencyIncrementDecrement() {
        final ScalingState state = new ScalingState(12);
        assertEquals(1, state.getTargetConcurrency().get());
        state.getTargetConcurrency().incrementAndGet();
        assertEquals(2, state.getTargetConcurrency().get());
        state.getTargetConcurrency().decrementAndGet();
        assertEquals(1, state.getTargetConcurrency().get());
    }

    @Test
    void testPendingValidation() {
        final ScalingState state = new ScalingState(12);
        assertFalse(state.isPendingValidation());
        state.setPendingValidation(true);
        assertTrue(state.isPendingValidation());
        state.setPendingValidation(false);
        assertFalse(state.isPendingValidation());
    }

    @Test
    void testThroughputBaselineTracking() {
        final ScalingState state = new ScalingState(12);
        state.setPreScaleUpFlowFilesRate(100);
        state.setPreScaleUpBytesRate(5000);
        assertEquals(100, state.getPreScaleUpFlowFilesRate());
        assertEquals(5000, state.getPreScaleUpBytesRate());
    }

    @Test
    void testEventRepositoryTracking() {
        final ScalingState state = new ScalingState(12);
        state.setPreviousFlowFilesOut(1000);
        state.setPreviousBytesOut(50000);
        assertEquals(1000, state.getPreviousFlowFilesOut());
        assertEquals(50000, state.getPreviousBytesOut());
    }
}
