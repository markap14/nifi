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
package org.apache.nifi.controller.scheduling.auto;

import org.apache.nifi.controller.scheduling.SchedulingSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardAutoSchedulingControllerTest {
    private static final NodeSchedulingObservation AVAILABLE_NODE = new NodeSchedulingObservation(true, 0.25D, 0.25D, 0D, 32, 0D, 0D, false);

    @Test
    void testQueuedWorkCanAcquireMoreCapacityBeforeFirstCompletion() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.completedInvocations = 0;
        simulation.observe(0);
        assertEquals(2, simulation.settings.concurrentTasks());

        for (int second = 0; second < 15; second++) {
            simulation.observe(0);
            assertTrue(simulation.settings.concurrentTasks() <= 2);
        }

        simulation.completedInvocations = 1;
        simulation.observe(1);
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @ParameterizedTest
    @CsvSource({"1000, 1", "2000, 2"})
    void testCatchingUpWithDemandRequiresThroughputGain(final long candidateWork, final int expectedConcurrency) {
        final Simulation simulation = new Simulation(12, true, false);
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(25), simulation.settings.runDurationNanos());

        simulation.inputReady = false;
        simulation.inputBacklogged = false;
        simulation.observe(6, candidateWork);
        assertEquals(expectedConcurrency, simulation.settings.concurrentTasks());

        simulation.occupancy = 0.5D;
        simulation.observe(1000);
        assertEquals(expectedConcurrency, simulation.settings.concurrentTasks());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 100, 1000})
    void testGrowthMustImproveUsefulThroughput(final long candidateWork) {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
        simulation.observe(6, candidateWork);

        assertEquals(1, simulation.settings.concurrentTasks());

        for (int second = 0; second < 5; second++) {
            simulation.observe(1000);
            assertEquals(1, simulation.settings.concurrentTasks());
        }
    }

    @Test
    void testSourceRequiresRecentDemand() {
        final Simulation source = new Simulation(12, false, false);
        source.inputBacklogged = false;
        source.sourceDemand = true;
        source.observe(100);
        assertEquals(2, source.settings.concurrentTasks());

        final Simulation idleSource = new Simulation(12, false, false);
        idleSource.inputBacklogged = false;
        idleSource.occupancy = 0D;
        idleSource.observe(0);
        assertEquals(1, idleSource.settings.concurrentTasks());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 16, 64})
    void testCapacityAdaptsToLocalCeiling(final int ceiling) {
        final Simulation simulation = new Simulation(ceiling, false, false);
        int maximumConcurrency = 1;
        for (int second = 0; second < 240; second++) {
            simulation.observe(simulation.settings.concurrentTasks() * 1000L);
            maximumConcurrency = Math.max(maximumConcurrency, simulation.settings.concurrentTasks());
            assertTrue(simulation.settings.concurrentTasks() <= ceiling);
        }

        assertEquals(ceiling, maximumConcurrency);
    }

    @Test
    void testSerialProcessorRemainsSerial() {
        final Simulation simulation = new Simulation(12, true, true);
        for (int second = 0; second < 10; second++) {
            simulation.observe(1000);
            assertEquals(1, simulation.settings.concurrentTasks());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCpuSaturationPreservesProductiveCapacity(final boolean duringGrowthProbe) {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.settings = new SchedulingSettings(duringGrowthProbe ? 1 : 4, 0L);
        if (duringGrowthProbe) {
            simulation.observe(1000);
        }

        final int expectedConcurrency = duringGrowthProbe ? 2 : 4;
        simulation.node = new NodeSchedulingObservation(true, 0.95D, 0.95D, 0D, 32, 0D, 0D, true);
        simulation.observe(15, expectedConcurrency * 1000L);
        assertEquals(expectedConcurrency, simulation.settings.concurrentTasks());
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "false, 1000"})
    void testIdleBackpressuredAndIntermittentWorkReleaseUnusedCapacity(final boolean backpressured, final long work) {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.settings = new SchedulingSettings(4, 0L);
        simulation.inputReady = !backpressured;
        simulation.inputBacklogged = backpressured || work > 0L;
        simulation.occupancy = backpressured ? 1D : 0.4D;
        simulation.blockedNanos = backpressured ? TimeUnit.SECONDS.toNanos(1) : 0L;
        simulation.observe(9, work);
        assertEquals(1, simulation.settings.concurrentTasks());

        simulation.inputReady = true;
        simulation.inputBacklogged = true;
        simulation.occupancy = 1D;
        simulation.blockedNanos = 0L;
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @Test
    void testReductionPreservesThroughputAndRestoresCapacityWhenNeeded() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.settings = new SchedulingSettings(4, 0L);
        simulation.occupancy = 0.5D;
        simulation.observe(45, 1000);

        assertTrue(simulation.settings.concurrentTasks() < 4);

        final Simulation busy = new Simulation(4, false, false);
        busy.settings = new SchedulingSettings(4, 0L);
        for (int second = 0; second < 40; second++) {
            busy.observe(busy.settings.concurrentTasks() * 1000L);
        }

        assertEquals(4, busy.settings.concurrentTasks());
    }

    @Test
    void testUnavailableCpuMeasurementStillAllowsBoundedProbing() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.node = new NodeSchedulingObservation(false, -1D, -1D, 0D, 32, 0D, 0D, false);
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
    }

    @Test
    void testSharedBudgetAndFailuresBoundGrowth() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.node = new NodeSchedulingObservation(true, 0.25D, 0.25D, 0D, 1, 1D, 0.5D, false);
        simulation.observe(1000);
        assertEquals(1, simulation.settings.concurrentTasks());
        simulation.node = AVAILABLE_NODE;
        simulation.observe(1000);
        assertEquals(2, simulation.settings.concurrentTasks());
        simulation.failures = 1;
        simulation.observe(1000);
        assertEquals(1, simulation.settings.concurrentTasks());
        simulation.observe(1000);
        assertEquals(AutoDecisionReason.PROCESSOR_FAILURE, simulation.lastDecision.reason());
    }

    @Test
    void testProbeHasBoundedLifetimeWithoutCompletions() {
        final Simulation simulation = new Simulation(12, false, false);
        simulation.completedInvocations = 0;
        simulation.observe(0);
        simulation.observe(30, 0);

        assertNotEquals(AutoControllerPhase.TRIAL, simulation.lastDecision.phase());
        assertEquals(1, simulation.settings.concurrentTasks());
    }

    @Test
    void testSlowSerializedWorkDoesNotAccumulateUnproductiveConcurrency() {
        final Simulation simulation = new Simulation(12, false, false);
        int maximumConcurrency = 1;
        boolean reducedToOne = false;
        for (int second = 1; second <= 120; second++) {
            final boolean completed = second % 5 == 0;
            simulation.completedInvocations = completed ? 1 : 0;
            simulation.invocationNanos = completed ? TimeUnit.SECONDS.toNanos(5L * simulation.settings.concurrentTasks()) : 0L;
            simulation.observe(completed ? 100 : 0);
            maximumConcurrency = Math.max(maximumConcurrency, simulation.settings.concurrentTasks());
            if (second > 60 && simulation.settings.concurrentTasks() == 1) {
                reducedToOne = true;
            }
        }

        assertTrue(maximumConcurrency <= 3);
        assertTrue(reducedToOne);
    }

    @Test
    void testScalableWorkloadRampsDespiteVariableCommitRates() {
        final Simulation simulation = new Simulation(16, false, false);
        final double[] relativeRates = {1.3D, 0.7D, 1.2D, 0.8D, 1.1D, 0.9D};
        int maximumConcurrency = 1;
        for (int second = 0; second < 90; second++) {
            simulation.observe((long) (1000 * simulation.settings.concurrentTasks() * relativeRates[second % relativeRates.length]));
            maximumConcurrency = Math.max(maximumConcurrency, simulation.settings.concurrentTasks());
        }

        assertEquals(16, maximumConcurrency);
    }

    private static class Simulation {
        private final StandardAutoSchedulingController controller;
        private SchedulingSettings settings;
        private NodeSchedulingObservation node = AVAILABLE_NODE;
        private AutoSchedulingDecision lastDecision;
        private long timestampNanos;
        private long completedInvocations = 10;
        private long failures;
        private long invocationNanos;
        private long blockedNanos;
        private double occupancy = 1D;
        private boolean inputReady = true;
        private boolean inputBacklogged = true;
        private boolean sourceDemand;

        private Simulation(final int ceiling, final boolean batching, final boolean serial) {
            controller = new StandardAutoSchedulingController(ceiling, batching, serial);
            node = new NodeSchedulingObservation(true, 0.25D, 0.25D, 0D, Math.max(32, ceiling), 0D, 0D, false);
            settings = new SchedulingSettings(1, batching ? TimeUnit.MILLISECONDS.toNanos(25) : 0L);
        }

        private void observe(final int seconds, final long work) {
            for (int second = 0; second < seconds; second++) {
                observe(work);
            }
        }

        private void observe(final long work) {
            timestampNanos += TimeUnit.SECONDS.toNanos(1);
            final AutoSchedulingObservation observation = new AutoSchedulingObservation(timestampNanos, TimeUnit.SECONDS.toNanos(1), settings, work,
                    completedInvocations, failures, invocationNanos, blockedNanos, 0L, occupancy, inputReady, inputBacklogged, 0D, sourceDemand, false);
            lastDecision = controller.evaluate(observation, node);
            if (lastDecision.applySettings()) {
                settings = lastDecision.settings();
            }
        }
    }
}
