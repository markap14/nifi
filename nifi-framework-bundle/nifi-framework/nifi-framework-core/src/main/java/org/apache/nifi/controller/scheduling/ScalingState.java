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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Maintains per-processor state for auto-scaling decisions. A fresh instance (with
 * targetConcurrency = 1) is created each time a processor is scheduled, so that
 * scaling always starts from 1 after a stop/start cycle.
 * <p>
 * State that is mutated during scaling evaluation (cooldown, idle-hold, rate snapshots,
 * pending-validation, pre-scale-up rate) is expected to be accessed only while holding
 * the instance monitor. Evaluation callers must synchronize on this instance before
 * reading or writing those fields. Fields that are updated outside of evaluation
 * (targetConcurrency, invocationCounter) use atomic types so that every invocation
 * thread can update them without locking.
 */
class ScalingState {
    private static final long EVALUATION_INTERVAL_MILLIS = 1000L;
    private static final long COOLDOWN_DURATION_MILLIS = 10_000L;

    private final AtomicInteger targetConcurrency = new AtomicInteger(1);
    private final AtomicLong invocationCounter = new AtomicLong(0);
    private final int maxConcurrency;

    private long lastEvaluationTime = 0;
    private long previousInvocationSnapshot = 0;
    private long previousInvocationSnapshotNanos = 0;
    private double preScaleUpInvocationsPerSecond = 0.0;
    private boolean pendingValidation = false;
    private long cooldownExpiration = 0;
    private long idleHoldSince = 0;

    ScalingState(final int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    AtomicInteger getTargetConcurrency() {
        return targetConcurrency;
    }

    int getMaxConcurrency() {
        return maxConcurrency;
    }

    /**
     * Increments the invocation counter. Called by every scheduling thread after each
     * successful invocation of the connectable. The counter is used by the scaler to
     * compute an actual per-second invocation rate from the delta between consecutive
     * evaluations.
     */
    void recordInvocation() {
        invocationCounter.incrementAndGet();
    }

    long getInvocationCount() {
        return invocationCounter.get();
    }

    synchronized long getLastEvaluationTime() {
        return lastEvaluationTime;
    }

    synchronized void setLastEvaluationTime(final long lastEvaluationTime) {
        this.lastEvaluationTime = lastEvaluationTime;
    }

    synchronized long getPreviousInvocationSnapshot() {
        return previousInvocationSnapshot;
    }

    synchronized long getPreviousInvocationSnapshotNanos() {
        return previousInvocationSnapshotNanos;
    }

    synchronized void updateInvocationSnapshot(final long invocationCount, final long nanoTime) {
        this.previousInvocationSnapshot = invocationCount;
        this.previousInvocationSnapshotNanos = nanoTime;
    }

    synchronized double getPreScaleUpInvocationsPerSecond() {
        return preScaleUpInvocationsPerSecond;
    }

    synchronized void setPreScaleUpInvocationsPerSecond(final double preScaleUpInvocationsPerSecond) {
        this.preScaleUpInvocationsPerSecond = preScaleUpInvocationsPerSecond;
    }

    synchronized boolean isPendingValidation() {
        return pendingValidation;
    }

    synchronized void setPendingValidation(final boolean pendingValidation) {
        this.pendingValidation = pendingValidation;
    }

    synchronized long getCooldownExpiration() {
        return cooldownExpiration;
    }

    synchronized void setCooldownExpiration(final long cooldownExpiration) {
        this.cooldownExpiration = cooldownExpiration;
    }

    synchronized long getIdleHoldSince() {
        return idleHoldSince;
    }

    synchronized void setIdleHoldSince(final long idleHoldSince) {
        this.idleHoldSince = idleHoldSince;
    }

    /**
     * Atomically claims the current evaluation window if the configured interval has
     * elapsed since the last successful claim. Returns true only for the single caller
     * that wins the race.
     */
    synchronized boolean tryClaimEvaluation() {
        final long now = System.currentTimeMillis();
        if (now - lastEvaluationTime < EVALUATION_INTERVAL_MILLIS) {
            return false;
        }
        lastEvaluationTime = now;
        return true;
    }

    synchronized boolean isInCooldown() {
        return System.currentTimeMillis() < cooldownExpiration;
    }

    synchronized void enterCooldown() {
        this.cooldownExpiration = System.currentTimeMillis() + COOLDOWN_DURATION_MILLIS;
    }
}
