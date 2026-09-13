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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.List;
import java.util.function.LongSupplier;

public class NodeSchedulingObservationSampler {
    private static final double NEWEST_SAMPLE_WEIGHT = 0.25D;
    private static final double CPU_GUARD_THRESHOLD = 0.90D;
    private static final double CPU_GUARD_RECOVERY_THRESHOLD = 0.80D;

    private final OperatingSystemMXBean operatingSystemMXBean;
    private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;
    private final LongSupplier nanoTimeSupplier;

    private double smoothedCpuLoad = -1D;
    private long previousSampleNanos;
    private long previousGarbageCollectionMillis;
    private long previousProcessCpuNanos;
    private int highGarbageCollectionSamples;
    private int lowCpuSamples;
    private boolean cpuGuardEngaged;
    private boolean garbageCollectionGuardEngaged;

    public NodeSchedulingObservationSampler() {
        this(ManagementFactory.getOperatingSystemMXBean(), ManagementFactory.getGarbageCollectorMXBeans(), System::nanoTime);
    }

    NodeSchedulingObservationSampler(final OperatingSystemMXBean operatingSystemMXBean,
                                     final List<GarbageCollectorMXBean> garbageCollectorMXBeans, final LongSupplier nanoTimeSupplier) {
        this.operatingSystemMXBean = operatingSystemMXBean;
        this.garbageCollectorMXBeans = List.copyOf(garbageCollectorMXBeans);
        this.nanoTimeSupplier = nanoTimeSupplier;
        previousSampleNanos = nanoTimeSupplier.getAsLong();
        previousGarbageCollectionMillis = getGarbageCollectionMillis();
        previousProcessCpuNanos = getProcessCpuTime();
    }

    public NodeSchedulingObservation sample(final int globalBudget, final double globalUtilization, final double globalContention) {
        final long nowNanos = nanoTimeSupplier.getAsLong();
        final long garbageCollectionMillis = getGarbageCollectionMillis();
        final long elapsedNanos = Math.max(1L, nowNanos - previousSampleNanos);
        final long garbageCollectionDeltaMillis = Math.max(0L, garbageCollectionMillis - previousGarbageCollectionMillis);
        final double garbageCollectionOverhead = garbageCollectionDeltaMillis / (elapsedNanos / 1_000_000D);

        previousSampleNanos = nowNanos;
        previousGarbageCollectionMillis = garbageCollectionMillis;

        if (garbageCollectionOverhead >= 0.10D) {
            highGarbageCollectionSamples++;
        } else {
            highGarbageCollectionSamples = 0;
            garbageCollectionGuardEngaged = false;
        }
        if (highGarbageCollectionSamples >= 2) {
            garbageCollectionGuardEngaged = true;
        }

        final double cpuLoad = getCpuLoad(elapsedNanos);
        final boolean cpuAvailable = cpuLoad >= 0D;
        if (cpuAvailable) {
            smoothedCpuLoad = smoothedCpuLoad < 0D ? cpuLoad : NEWEST_SAMPLE_WEIGHT * cpuLoad + (1D - NEWEST_SAMPLE_WEIGHT) * smoothedCpuLoad;
            if (smoothedCpuLoad >= CPU_GUARD_THRESHOLD) {
                cpuGuardEngaged = true;
                lowCpuSamples = 0;
            } else if (smoothedCpuLoad < CPU_GUARD_RECOVERY_THRESHOLD) {
                lowCpuSamples++;
                if (lowCpuSamples >= 2) {
                    cpuGuardEngaged = false;
                }
            } else {
                lowCpuSamples = 0;
            }
        } else {
            smoothedCpuLoad = -1D;
            cpuGuardEngaged = false;
            lowCpuSamples = 0;
        }

        return new NodeSchedulingObservation(cpuAvailable, cpuLoad, smoothedCpuLoad, garbageCollectionOverhead, globalBudget,
                globalUtilization, globalContention, cpuGuardEngaged || garbageCollectionGuardEngaged);
    }

    private double getCpuLoad(final long elapsedNanos) {
        if (operatingSystemMXBean instanceof final com.sun.management.OperatingSystemMXBean extendedOperatingSystemMXBean) {
            final double cpuLoad = Math.max(extendedOperatingSystemMXBean.getCpuLoad(), extendedOperatingSystemMXBean.getProcessCpuLoad());
            if (cpuLoad >= 0D) {
                previousProcessCpuNanos = extendedOperatingSystemMXBean.getProcessCpuTime();
                return cpuLoad;
            }

            final long processCpuNanos = extendedOperatingSystemMXBean.getProcessCpuTime();
            final long processCpuDeltaNanos = previousProcessCpuNanos < 0L ? -1L : processCpuNanos - previousProcessCpuNanos;
            previousProcessCpuNanos = processCpuNanos;
            if (processCpuDeltaNanos >= 0L) {
                return Math.min(1D, processCpuDeltaNanos / (double) (elapsedNanos * operatingSystemMXBean.getAvailableProcessors()));
            }
        }

        return -1D;
    }

    private long getProcessCpuTime() {
        return operatingSystemMXBean instanceof final com.sun.management.OperatingSystemMXBean extendedOperatingSystemMXBean
                ? extendedOperatingSystemMXBean.getProcessCpuTime() : -1L;
    }

    private long getGarbageCollectionMillis() {
        long totalMillis = 0L;
        for (final GarbageCollectorMXBean garbageCollectorMXBean : garbageCollectorMXBeans) {
            final long collectionTime = garbageCollectorMXBean.getCollectionTime();
            if (collectionTime > 0L) {
                totalMillis += collectionTime;
            }
        }

        return totalMillis;
    }
}
