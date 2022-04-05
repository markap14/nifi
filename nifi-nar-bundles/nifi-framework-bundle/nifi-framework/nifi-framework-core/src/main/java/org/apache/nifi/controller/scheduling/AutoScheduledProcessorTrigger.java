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

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class AutoScheduledProcessorTrigger implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AutoScheduledProcessorTrigger.class);

    private static final int MAX_ACTIVE_THREAD_COUNT = 20;
    private static final int MAX_ITERATIONS_PER_TRIGGER = 500000;
    private static final int MAX_ITERATIONS_SOURCE_PROCESSOR = 500;
    private static final long MAX_RUN_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(25L);

    private final ProcessorTaskQueue taskQueue;
    private final long boredYieldMillis = 5;
    private volatile boolean stopped = false;

    public AutoScheduledProcessorTrigger(final ProcessorTaskQueue taskQueue) {
        this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                // TODO: Account for @TriggerSerially
                // TODO: Allow processors to indicate default Run Schedule, and then take that into account
                // TODO: Look into using:
                //       ManagementFactory.getThreadMXBean().isCurrentThreadCpuTimeSupported() / ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime()
                //       in order to understand more about CPU usage. Call before/after each processor is triggered. Then add to stats.
                //       Then we know how much CPU time is being used by each processor. Potentially look at also checking lock contention, etc.
                //       If this is expensive then would need to be enabled/disabled using nifi.sh diagnostics --enable-cpu-tracking, etc.
                final boolean triggered = trigger();

                if (!triggered) {
                    try {
                        logger.trace("Triggered no processors. Will yield for {} millis.", boredYieldMillis);
                        Thread.sleep(boredYieldMillis);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } catch (final Exception e) {
                logger.error("Failed to trigger auto-scheduled processors", e);
            }
        }
    }

    public void stop() {
        stopped = true;
    }

    private boolean trigger() {
        final List<ProcessorTaskQueue.ProcessorTriggerContext> triggerContexts = taskQueue.getTriggerContexts();

        boolean triggered = false;
        for (final ProcessorTaskQueue.ProcessorTriggerContext triggerContext : triggerContexts) {
            try {
                if (isTrigger(triggerContext)) {
                    final long runDuration;
                    final long maxIterations;
                    if (triggerContext.isSourceComponent()) {
                        runDuration = MAX_RUN_DURATION_NANOS;
                        maxIterations = MAX_ITERATIONS_SOURCE_PROCESSOR;
                    } else {
                        runDuration = getRunDurationNanos(triggerContext);
                        maxIterations = MAX_ITERATIONS_PER_TRIGGER;
                    }

                    if (triggerContext.isBatchSupported()) {
                        final InvocationResult result = triggerContext.getConnectableTask().invoke(runDuration, maxIterations);

                        if (result.isYield()) {
                            triggerContext.getProcessor().yield(boredYieldMillis, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        triggerForDuration(triggerContext, runDuration, maxIterations);
                    }

                    triggered = true;
                }
            } catch (final Exception e) {
                logger.error("Failed to trigger {}", triggerContext.getProcessor(), e);
            }
        }

        return triggered;
    }

    private void triggerForDuration(final ProcessorTaskQueue.ProcessorTriggerContext triggerContext, final long runDuration, final long maxIterations) {
        final ConnectableTask connectableTask = triggerContext.getConnectableTask();

        final long stopTime = System.nanoTime() + runDuration;
        long count = 0L;
        while (count++ < maxIterations && System.nanoTime() < stopTime) {
            final InvocationResult result = connectableTask.invoke();
            if (result.isYield()) {
                triggerContext.getProcessor().yield(boredYieldMillis, TimeUnit.MILLISECONDS);
                return;
            }
        }
    }

    private boolean isTrigger(final ProcessorTaskQueue.ProcessorTriggerContext triggerContext) {
        final LifecycleState lifecycleState = triggerContext.getLifecycleState();
        final ProcessorNode processorNode = triggerContext.getProcessor();

        if (!lifecycleState.isScheduled()) {
            return false;
        }

        final long yieldExpiration = processorNode.getYieldExpiration();
        if (yieldExpiration > 0 && yieldExpiration > System.currentTimeMillis()) { // Check > 0 before System.currentTimeMillis() to avoid system call when we're able to
            return false;
        }

        final int activeThreadCount = lifecycleState.getActiveThreadCount();
        if (activeThreadCount >= MAX_ACTIVE_THREAD_COUNT) {
            return false;
        }

        if (!triggerContext.isWorkToDo()) {
            return false;
        }

        return true;
    }

    private long getRunDurationNanos(final ProcessorTaskQueue.ProcessorTriggerContext triggerContext) {
        return Math.max(1, (long) (triggerContext.getQueueFullRatio() * MAX_RUN_DURATION_NANOS));
    }
}
