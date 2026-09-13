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
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AutoSchedulingMetricsTest {
    @Test
    void testRetainedObserverReportsAllCommitsAcrossConcurrencyChanges() {
        final AutoSchedulingMetrics metrics = new AutoSchedulingMetrics();
        final InvocationObserver retainedObserver = metrics.createObserver();
        retainedObserver.onInvocationCompleted(InvocationResult.completed(InvocationOutcome.INVOKED_WITHOUT_ACTIVITY));
        for (int second = 1; second <= 10; second++) {
            retainedObserver.onCommit(new CommittedSchedulingWork(0, 100));
            final AutoSchedulingObservation observation = snapshot(metrics, second);
            assertEquals(100, observation.committedFlowFiles());
        }
    }

    @Test
    void testOnlyCommittedWorkContributesToThroughput() {
        final AutoSchedulingMetrics metrics = new AutoSchedulingMetrics();
        final InvocationObserver observer = metrics.createObserver();
        observer.onActivity();
        observer.onCommitFailure();
        observer.onInvocationCompleted(InvocationResult.completed(InvocationOutcome.FAILED));
        final AutoSchedulingObservation failed = snapshot(metrics, 1);
        assertEquals(0, failed.committedFlowFiles());
        assertEquals(2, failed.failures());
        observer.onCommit(new CommittedSchedulingWork(2, 10));
        assertEquals(2, snapshot(metrics, 2).committedFlowFiles());
        assertEquals(0, snapshot(metrics, 3).committedFlowFiles());
    }

    @Test
    void testLongInvocationsContributeOccupancyBeforeCompleting() {
        final AutoSchedulingMetrics metrics = new AutoSchedulingMetrics();
        metrics.recordControlSample(2, 2, null, TimeUnit.SECONDS.toNanos(1));
        final AutoSchedulingObservation observation = snapshot(metrics, 2);
        assertEquals(1D, observation.activeOccupancy());
        assertEquals(0, observation.completedInvocations());
    }

    @Test
    void testBatchActivityDistinguishesEachTrigger() {
        final InvocationObserver observer = new AutoSchedulingMetrics().createObserver();
        observer.onActivity();
        assertTrue(observer.isTriggerActivityObserved());
        observer.resetTriggerActivity();
        assertFalse(observer.isTriggerActivityObserved());
        assertTrue(observer.isActivityObserved());
    }

    private AutoSchedulingObservation snapshot(final AutoSchedulingMetrics metrics, final int concurrency) {
        return metrics.snapshot(System.nanoTime(), TimeUnit.SECONDS.toNanos(1), new SchedulingSettings(concurrency, 0L), 0D, true, true, 0D, false, false);
    }
}
