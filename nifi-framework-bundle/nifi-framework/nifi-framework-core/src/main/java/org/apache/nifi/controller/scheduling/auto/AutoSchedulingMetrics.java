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

import org.apache.nifi.controller.scheduling.CommittedSchedulingWork;
import org.apache.nifi.controller.scheduling.SchedulingSettings;
import org.apache.nifi.controller.tasks.InvocationObserver;
import org.apache.nifi.controller.tasks.InvocationOutcome;
import org.apache.nifi.controller.tasks.InvocationResult;

import java.util.concurrent.atomic.LongAdder;

public class AutoSchedulingMetrics {
    private final LongAdder committedFlowFiles = new LongAdder();
    private final LongAdder completedInvocations = new LongAdder();
    private final LongAdder failures = new LongAdder();
    private final LongAdder invocationNanos = new LongAdder();
    private long previousWork;
    private long previousInvocations;
    private long previousFailures;
    private long previousInvocationNanos;
    private long sampledNanos;
    private double occupiedNanos;
    private long outputBlockedNanos;
    private long yieldedNanos;

    public InvocationObserver createObserver() {
        return new ObservationToken();
    }

    public void recordInvocationDuration(final long nanos) {
        invocationNanos.add(Math.max(0L, nanos));
    }

    public void recordControlSample(final int activeInvocations, final int selectedConcurrency, final InvocationOutcome readinessOutcome, final long sampleDurationNanos) {
        sampledNanos += sampleDurationNanos;
        occupiedNanos += Math.min(1D, activeInvocations / (double) selectedConcurrency) * sampleDurationNanos;
        if (readinessOutcome == InvocationOutcome.BACKPRESSURED) {
            outputBlockedNanos += sampleDurationNanos;
        } else if (readinessOutcome == InvocationOutcome.YIELDED) {
            yieldedNanos += sampleDurationNanos;
        }
    }

    public AutoSchedulingObservation snapshot(final long timestampNanos, final long durationNanos, final SchedulingSettings settings, final double activeOccupancy,
                                              final boolean inputReady, final boolean inputBacklogged, final double backlogTrend, final boolean sourceDemand, final boolean primaryNodeChanged) {
        final long work = committedFlowFiles.sum();
        final long invocations = completedInvocations.sum();
        final long failureCount = failures.sum();
        final long invocationDuration = invocationNanos.sum();
        final double occupancy = sampledNanos == 0L ? activeOccupancy : occupiedNanos / sampledNanos;
        final AutoSchedulingObservation observation = new AutoSchedulingObservation(timestampNanos, durationNanos, settings, work - previousWork,
                invocations - previousInvocations, failureCount - previousFailures, invocationDuration - previousInvocationNanos,
                outputBlockedNanos, yieldedNanos, occupancy, inputReady, inputBacklogged, backlogTrend, sourceDemand, primaryNodeChanged);
        previousWork = work;
        previousInvocations = invocations;
        previousFailures = failureCount;
        previousInvocationNanos = invocationDuration;
        sampledNanos = 0L;
        occupiedNanos = 0D;
        outputBlockedNanos = 0L;
        yieldedNanos = 0L;
        return observation;
    }

    private class ObservationToken implements InvocationObserver {
        private volatile boolean activity;
        private volatile boolean triggerActivity;

        @Override
        public boolean isActivityObserved() {
            return activity;
        }

        @Override
        public void resetTriggerActivity() {
            triggerActivity = false;
        }

        @Override
        public boolean isTriggerActivityObserved() {
            return triggerActivity;
        }

        @Override
        public void onInvocationCompleted(final InvocationResult result) {
            final InvocationOutcome outcome = result.getOutcome();
            if (outcome == InvocationOutcome.INVOKED_WITH_ACTIVITY || outcome == InvocationOutcome.INVOKED_WITHOUT_ACTIVITY || outcome == InvocationOutcome.FAILED) {
                completedInvocations.increment();
            }

            if (outcome == InvocationOutcome.FAILED) {
                failures.increment();
            }
        }

        @Override
        public void onActivity() {
            activity = true;
            triggerActivity = true;
        }

        @Override
        public void onCommit(final CommittedSchedulingWork work) {
            // The counters belong to the scheduled processor, not its current task count. Retained factories and asynchronous commits remain observable.
            final long count = work.inputFlowFiles() > 0L ? work.inputFlowFiles() : work.producedFlowFiles();
            committedFlowFiles.add(count);
            if (count > 0L) {
                onActivity();
            }
        }

        @Override
        public void onCommitFailure() {
            failures.increment();
        }
    }
}
