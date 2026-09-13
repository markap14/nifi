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

import com.sun.management.OperatingSystemMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeSchedulingObservationSamplerTest {
    private static final int GLOBAL_BUDGET = 32;

    @Test
    void testCpuGuardUsesSmoothedLoadAndRecoverySamples() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(0.995D, 0.50D, 0.50D);
        final AtomicLong nowNanos = new AtomicLong();
        final NodeSchedulingObservationSampler sampler = new NodeSchedulingObservationSampler(operatingSystemMXBean, List.of(), nowNanos::get);

        assertTrue(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
        assertTrue(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
        assertTrue(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
        assertFalse(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
    }

    @Test
    void testGarbageCollectionGuardRequiresTwoHighSamples() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(0.25D);
        final GarbageCollectorMXBean garbageCollectorMXBean = mock(GarbageCollectorMXBean.class);
        when(garbageCollectorMXBean.getCollectionTime()).thenReturn(0L, 100L, 200L);
        final AtomicLong nowNanos = new AtomicLong();
        final NodeSchedulingObservationSampler sampler = new NodeSchedulingObservationSampler(
                operatingSystemMXBean, List.of(garbageCollectorMXBean), nowNanos::get);

        assertFalse(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
        assertTrue(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
    }

    @Test
    void testSystemCpuPressureAlsoLimitsGrowth() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getCpuLoad()).thenReturn(1D);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(0.25D);
        final AtomicLong nowNanos = new AtomicLong();
        final NodeSchedulingObservationSampler sampler = new NodeSchedulingObservationSampler(operatingSystemMXBean, List.of(), nowNanos::get);

        final NodeSchedulingObservation observation = sampleAfterOneSecond(sampler, nowNanos);

        assertEquals(1D, observation.cpuLoad());
        assertTrue(observation.resourceGuardEngaged());
    }

    @Test
    void testUnavailableMeasurementDoesNotRetainAnOldCpuGuard() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getCpuLoad()).thenReturn(1D, -1D);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(-1D);
        when(operatingSystemMXBean.getProcessCpuTime()).thenReturn(-1L);
        final AtomicLong nowNanos = new AtomicLong();
        final NodeSchedulingObservationSampler sampler = new NodeSchedulingObservationSampler(operatingSystemMXBean, List.of(), nowNanos::get);

        assertTrue(sampleAfterOneSecond(sampler, nowNanos).resourceGuardEngaged());
        final NodeSchedulingObservation unavailable = sampleAfterOneSecond(sampler, nowNanos);
        assertFalse(unavailable.cpuAvailable());
        assertFalse(unavailable.resourceGuardEngaged());
    }

    @Test
    void testProcessCpuTimeFallback() {
        final OperatingSystemMXBean operatingSystemMXBean = mock(OperatingSystemMXBean.class);
        when(operatingSystemMXBean.getCpuLoad()).thenReturn(-1D);
        when(operatingSystemMXBean.getProcessCpuLoad()).thenReturn(-1D);
        when(operatingSystemMXBean.getProcessCpuTime()).thenReturn(0L, TimeUnit.SECONDS.toNanos(2L));
        when(operatingSystemMXBean.getAvailableProcessors()).thenReturn(4);
        final AtomicLong nowNanos = new AtomicLong();
        final NodeSchedulingObservationSampler sampler = new NodeSchedulingObservationSampler(operatingSystemMXBean, List.of(), nowNanos::get);

        final NodeSchedulingObservation observation = sampleAfterOneSecond(sampler, nowNanos);

        assertTrue(observation.cpuAvailable());
        assertEquals(0.50D, observation.cpuLoad());
    }

    private NodeSchedulingObservation sampleAfterOneSecond(final NodeSchedulingObservationSampler sampler, final AtomicLong nowNanos) {
        nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(1L));
        return sampler.sample(GLOBAL_BUDGET, 0.25D, 0D);
    }
}
