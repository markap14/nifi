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

/**
 * Maintains per-processor state for auto-scaling decisions. A fresh instance (with
 * targetConcurrency = 1) is created each time a processor is scheduled, so that
 * scaling always starts from 1 after a stop/start cycle.
 */
class ScalingState {
    private static final long EVALUATION_INTERVAL_MILLIS = 1000L;
    private static final long COOLDOWN_DURATION_MILLIS = 10_000L;

    private final AtomicInteger targetConcurrency = new AtomicInteger(1);
    private final int maxConcurrency;

    private volatile long lastEvaluationTime = 0;
    private volatile long previousFlowFilesOut = 0;
    private volatile long previousBytesOut = 0;
    private volatile long preScaleUpFlowFilesRate = 0;
    private volatile long preScaleUpBytesRate = 0;
    private volatile boolean pendingValidation = false;
    private volatile long cooldownExpiration = 0;
    private volatile long idleHoldSince = 0;

    ScalingState(final int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    AtomicInteger getTargetConcurrency() {
        return targetConcurrency;
    }

    int getMaxConcurrency() {
        return maxConcurrency;
    }

    long getLastEvaluationTime() {
        return lastEvaluationTime;
    }

    void setLastEvaluationTime(final long lastEvaluationTime) {
        this.lastEvaluationTime = lastEvaluationTime;
    }

    long getPreviousFlowFilesOut() {
        return previousFlowFilesOut;
    }

    void setPreviousFlowFilesOut(final long previousFlowFilesOut) {
        this.previousFlowFilesOut = previousFlowFilesOut;
    }

    long getPreviousBytesOut() {
        return previousBytesOut;
    }

    void setPreviousBytesOut(final long previousBytesOut) {
        this.previousBytesOut = previousBytesOut;
    }

    long getPreScaleUpFlowFilesRate() {
        return preScaleUpFlowFilesRate;
    }

    void setPreScaleUpFlowFilesRate(final long preScaleUpFlowFilesRate) {
        this.preScaleUpFlowFilesRate = preScaleUpFlowFilesRate;
    }

    long getPreScaleUpBytesRate() {
        return preScaleUpBytesRate;
    }

    void setPreScaleUpBytesRate(final long preScaleUpBytesRate) {
        this.preScaleUpBytesRate = preScaleUpBytesRate;
    }

    boolean isPendingValidation() {
        return pendingValidation;
    }

    void setPendingValidation(final boolean pendingValidation) {
        this.pendingValidation = pendingValidation;
    }

    long getCooldownExpiration() {
        return cooldownExpiration;
    }

    void setCooldownExpiration(final long cooldownExpiration) {
        this.cooldownExpiration = cooldownExpiration;
    }

    long getIdleHoldSince() {
        return idleHoldSince;
    }

    void setIdleHoldSince(final long idleHoldSince) {
        this.idleHoldSince = idleHoldSince;
    }

    boolean isEvaluationDue() {
        return System.currentTimeMillis() - lastEvaluationTime >= EVALUATION_INTERVAL_MILLIS;
    }

    boolean isInCooldown() {
        return System.currentTimeMillis() < cooldownExpiration;
    }

    void enterCooldown() {
        this.cooldownExpiration = System.currentTimeMillis() + COOLDOWN_DURATION_MILLIS;
    }
}
