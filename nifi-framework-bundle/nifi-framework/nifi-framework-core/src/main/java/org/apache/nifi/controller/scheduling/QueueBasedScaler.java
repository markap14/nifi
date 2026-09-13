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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scaling strategy based on inbound queue depth, outbound backpressure ratio, and invocation
 * throughput. After each scale-up, the next evaluation verifies that the per-second invocation
 * rate actually improved; if not, the scale-up is rolled back and a cooldown period prevents
 * further scale-ups for a time.
 * <p>
 * The invocation rate is derived from a local counter on {@link ScalingState} that each
 * scheduling thread increments after every successful invocation. Using a locally maintained
 * counter yields a true per-second rate between consecutive evaluations, independent of the
 * rolling-window semantics of the framework's FlowFile event repository.
 */
class QueueBasedScaler implements VirtualThreadScaler {
    private static final Logger logger = LoggerFactory.getLogger(QueueBasedScaler.class);

    private static final double OUTBOUND_PRESSURE_THRESHOLD = 0.8;
    private static final double IDLE_INBOUND_PRESSURE_THRESHOLD = 0.4;
    private static final long IDLE_HOLD_BACKOFF_MILLIS = 3000L;

    @Override
    public ScalingRecommendation evaluate(final Connectable connectable, final ScalingState scalingState) {
        synchronized (scalingState) {
            return evaluateInternal(connectable, scalingState);
        }
    }

    private ScalingRecommendation evaluateInternal(final Connectable connectable, final ScalingState scalingState) {
        final long currentInvocationCount = scalingState.getInvocationCount();
        final long nowNanos = System.nanoTime();
        final long previousInvocationCount = scalingState.getPreviousInvocationSnapshot();
        final long previousNanos = scalingState.getPreviousInvocationSnapshotNanos();
        scalingState.updateInvocationSnapshot(currentInvocationCount, nowNanos);

        final double invocationsPerSecond;
        if (previousNanos == 0 || nowNanos <= previousNanos) {
            invocationsPerSecond = 0.0;
        } else {
            final long deltaInvocations = currentInvocationCount - previousInvocationCount;
            final long elapsedNanos = nowNanos - previousNanos;
            invocationsPerSecond = deltaInvocations * 1_000_000_000.0 / elapsedNanos;
        }

        if (scalingState.isPendingValidation()) {
            validateScaleUp(connectable, scalingState, invocationsPerSecond);
        }

        long currentInboundSize = 0;
        final boolean hasInbound = connectable.hasIncomingConnection();
        if (hasInbound) {
            for (final Connection connection : connectable.getIncomingConnections()) {
                if (connection.getSource() != connectable) {
                    currentInboundSize += connection.getFlowFileQueue().size().getObjectCount();
                }
            }
        }

        final double effectiveOutboundPressure = computeEffectiveOutboundPressure(connectable);
        final int currentTarget = scalingState.getTargetConcurrency().get();

        if (shouldScaleDown(currentTarget, effectiveOutboundPressure, currentInboundSize, hasInbound)) {
            scalingState.setIdleHoldSince(0);
            return ScalingRecommendation.SCALE_DOWN;
        }

        if (shouldScaleUp(currentTarget, scalingState, effectiveOutboundPressure, currentInboundSize, hasInbound)) {
            scalingState.setPreScaleUpInvocationsPerSecond(invocationsPerSecond);
            scalingState.setIdleHoldSince(0);
            return ScalingRecommendation.SCALE_UP;
        }

        if (currentTarget > 1 && hasInbound && computeMaxInboundPressure(connectable) < IDLE_INBOUND_PRESSURE_THRESHOLD) {
            final long now = System.currentTimeMillis();
            if (scalingState.getIdleHoldSince() == 0) {
                scalingState.setIdleHoldSince(now);
            } else if (now - scalingState.getIdleHoldSince() >= IDLE_HOLD_BACKOFF_MILLIS) {
                scalingState.setIdleHoldSince(now);
                logger.debug("Recommending scale-down for {} due to idle inbound queues (pressure below {} for {}ms)",
                        connectable.getName(), IDLE_INBOUND_PRESSURE_THRESHOLD, IDLE_HOLD_BACKOFF_MILLIS);
                return ScalingRecommendation.SCALE_DOWN;
            }
        } else {
            scalingState.setIdleHoldSince(0);
        }

        return ScalingRecommendation.HOLD;
    }

    void validateScaleUp(final Connectable connectable, final ScalingState scalingState, final double invocationsPerSecond) {
        scalingState.setPendingValidation(false);
        final double preScaleUpRate = scalingState.getPreScaleUpInvocationsPerSecond();
        final boolean throughputImproved = invocationsPerSecond > preScaleUpRate;

        if (!throughputImproved) {
            final int previous = scalingState.getTargetConcurrency().getAndUpdate(concurrency -> concurrency > 1 ? concurrency - 1 : concurrency);
            scalingState.enterCooldown();
            logger.debug("Scale-up validation failed for {}: invocation rate did not improve ({} -> {} invocations/sec); "
                            + "rolling back from {} to {} and entering cooldown",
                    connectable.getName(), preScaleUpRate, invocationsPerSecond,
                    previous, scalingState.getTargetConcurrency().get());
        }
    }

    boolean shouldScaleDown(final int currentTarget, final double effectiveOutboundPressure, final long currentInboundSize, final boolean hasInbound) {
        if (currentTarget <= 1) {
            return false;
        }
        if (effectiveOutboundPressure >= OUTBOUND_PRESSURE_THRESHOLD) {
            return true;
        }
        return hasInbound && currentInboundSize == 0;
    }

    boolean shouldScaleUp(final int currentTarget, final ScalingState scalingState, final double effectiveOutboundPressure, final long currentInboundSize, final boolean hasInbound) {
        if (currentTarget >= scalingState.getMaxConcurrency()) {
            return false;
        }
        if (scalingState.isInCooldown()) {
            return false;
        }
        if (scalingState.isPendingValidation()) {
            return false;
        }
        if (effectiveOutboundPressure >= OUTBOUND_PRESSURE_THRESHOLD) {
            return false;
        }
        if (!hasInbound) {
            return true;
        }
        return currentInboundSize > 0;
    }

    private double computeMaxInboundPressure(final Connectable connectable) {
        double maxPressure = 0.0;
        for (final Connection connection : connectable.getIncomingConnections()) {
            if (connection.getSource() != connectable) {
                maxPressure = Math.max(maxPressure, connection.getFlowFileQueue().getBackPressureRatio());
            }
        }
        return maxPressure;
    }

    private double computeEffectiveOutboundPressure(final Connectable connectable) {
        double maxOutboundPressure = 0.0;
        double minOutboundPressure = Double.MAX_VALUE;
        boolean hasOutbound = false;

        for (final Connection connection : connectable.getConnections()) {
            if (connection.getDestination() == connectable) {
                continue;
            }
            hasOutbound = true;
            final double pressure = connection.getFlowFileQueue().getBackPressureRatio();
            maxOutboundPressure = Math.max(maxOutboundPressure, pressure);
            minOutboundPressure = Math.min(minOutboundPressure, pressure);
        }

        final boolean triggerWhenAnyAvailable = connectable.isTriggerWhenAnyDestinationAvailable();
        return triggerWhenAnyAvailable && hasOutbound ? minOutboundPressure : maxOutboundPressure;
    }
}
