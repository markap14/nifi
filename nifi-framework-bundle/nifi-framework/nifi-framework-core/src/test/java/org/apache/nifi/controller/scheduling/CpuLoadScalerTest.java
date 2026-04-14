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
import org.junit.jupiter.api.Test;

import java.lang.management.OperatingSystemMXBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CpuLoadScalerTest {

    @Test
    void testScaleUpAllowedWhenLoadBelowCores() {
        final VirtualThreadScaler delegate = mock(VirtualThreadScaler.class);
        final Connectable connectable = mock(Connectable.class);
        final ScalingState scalingState = new ScalingState(12);

        when(delegate.evaluate(connectable, scalingState)).thenReturn(ScalingRecommendation.SCALE_UP);

        final OperatingSystemMXBean osMxBean = mock(OperatingSystemMXBean.class);
        when(osMxBean.getSystemLoadAverage()).thenReturn(2.0);

        final CpuLoadScaler cpuLoadScaler = new CpuLoadScaler(delegate, osMxBean, 8);
        assertEquals(ScalingRecommendation.SCALE_UP, cpuLoadScaler.evaluate(connectable, scalingState));
    }

    @Test
    void testScaleUpVetoedWhenLoadExceedsCores() {
        final VirtualThreadScaler delegate = mock(VirtualThreadScaler.class);
        final Connectable connectable = mock(Connectable.class);
        final ScalingState scalingState = new ScalingState(12);

        when(delegate.evaluate(connectable, scalingState)).thenReturn(ScalingRecommendation.SCALE_UP);

        final OperatingSystemMXBean osMxBean = mock(OperatingSystemMXBean.class);
        when(osMxBean.getSystemLoadAverage()).thenReturn(10.0);

        final CpuLoadScaler cpuLoadScaler = new CpuLoadScaler(delegate, osMxBean, 8);
        assertEquals(ScalingRecommendation.HOLD, cpuLoadScaler.evaluate(connectable, scalingState));
    }

    @Test
    void testScaleUpAllowedWhenLoadEqualsCores() {
        final VirtualThreadScaler delegate = mock(VirtualThreadScaler.class);
        final Connectable connectable = mock(Connectable.class);
        final ScalingState scalingState = new ScalingState(12);

        when(delegate.evaluate(connectable, scalingState)).thenReturn(ScalingRecommendation.SCALE_UP);

        final OperatingSystemMXBean osMxBean = mock(OperatingSystemMXBean.class);
        when(osMxBean.getSystemLoadAverage()).thenReturn(8.0);

        final CpuLoadScaler cpuLoadScaler = new CpuLoadScaler(delegate, osMxBean, 8);
        assertEquals(ScalingRecommendation.SCALE_UP, cpuLoadScaler.evaluate(connectable, scalingState));
    }

    @Test
    void testScaleDownPassedThrough() {
        final VirtualThreadScaler delegate = mock(VirtualThreadScaler.class);
        final Connectable connectable = mock(Connectable.class);
        final ScalingState scalingState = new ScalingState(12);

        when(delegate.evaluate(connectable, scalingState)).thenReturn(ScalingRecommendation.SCALE_DOWN);

        final OperatingSystemMXBean osMxBean = mock(OperatingSystemMXBean.class);
        when(osMxBean.getSystemLoadAverage()).thenReturn(10.0);

        final CpuLoadScaler cpuLoadScaler = new CpuLoadScaler(delegate, osMxBean, 8);
        assertEquals(ScalingRecommendation.SCALE_DOWN, cpuLoadScaler.evaluate(connectable, scalingState));
    }

    @Test
    void testHoldPassedThrough() {
        final VirtualThreadScaler delegate = mock(VirtualThreadScaler.class);
        final Connectable connectable = mock(Connectable.class);
        final ScalingState scalingState = new ScalingState(12);

        when(delegate.evaluate(connectable, scalingState)).thenReturn(ScalingRecommendation.HOLD);

        final OperatingSystemMXBean osMxBean = mock(OperatingSystemMXBean.class);
        when(osMxBean.getSystemLoadAverage()).thenReturn(2.0);

        final CpuLoadScaler cpuLoadScaler = new CpuLoadScaler(delegate, osMxBean, 8);
        assertEquals(ScalingRecommendation.HOLD, cpuLoadScaler.evaluate(connectable, scalingState));
    }

    @Test
    void testScaleUpAllowedWhenLoadAverageUnavailable() {
        final VirtualThreadScaler delegate = mock(VirtualThreadScaler.class);
        final Connectable connectable = mock(Connectable.class);
        final ScalingState scalingState = new ScalingState(12);

        when(delegate.evaluate(connectable, scalingState)).thenReturn(ScalingRecommendation.SCALE_UP);

        final OperatingSystemMXBean osMxBean = mock(OperatingSystemMXBean.class);
        when(osMxBean.getSystemLoadAverage()).thenReturn(-1.0);

        final CpuLoadScaler cpuLoadScaler = new CpuLoadScaler(delegate, osMxBean, 8);
        assertEquals(ScalingRecommendation.SCALE_UP, cpuLoadScaler.evaluate(connectable, scalingState));
    }
}
