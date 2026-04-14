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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Decorator that wraps another {@link VirtualThreadScaler} and vetoes scale-up recommendations
 * when the system CPU load average exceeds the number of available processor cores. Scale-down
 * and hold recommendations from the delegate are passed through unchanged.
 */
class CpuLoadScaler implements VirtualThreadScaler {
    private static final Logger logger = LoggerFactory.getLogger(CpuLoadScaler.class);

    private final VirtualThreadScaler delegate;
    private final OperatingSystemMXBean operatingSystemMXBean;
    private final int availableProcessors;

    CpuLoadScaler(final VirtualThreadScaler delegate) {
        this(delegate, ManagementFactory.getOperatingSystemMXBean(), Runtime.getRuntime().availableProcessors());
    }

    CpuLoadScaler(final VirtualThreadScaler delegate, final OperatingSystemMXBean operatingSystemMXBean, final int availableProcessors) {
        this.delegate = delegate;
        this.operatingSystemMXBean = operatingSystemMXBean;
        this.availableProcessors = availableProcessors;
    }

    @Override
    public ScalingRecommendation evaluate(final Connectable connectable, final ScalingState scalingState) {
        final ScalingRecommendation recommendation = delegate.evaluate(connectable, scalingState);

        if (recommendation == ScalingRecommendation.SCALE_UP) {
            final double systemLoadAverage = operatingSystemMXBean.getSystemLoadAverage();
            if (systemLoadAverage >= 0 && systemLoadAverage > availableProcessors) {
                logger.debug("Vetoing scale-up for {} because system load average ({}) exceeds available processors ({})",
                        connectable, systemLoadAverage, availableProcessors);
                return ScalingRecommendation.HOLD;
            }
        }

        return recommendation;
    }
}
