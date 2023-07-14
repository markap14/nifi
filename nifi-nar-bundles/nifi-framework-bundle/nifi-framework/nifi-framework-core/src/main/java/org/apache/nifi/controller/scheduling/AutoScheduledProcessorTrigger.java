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
import org.apache.nifi.controller.tasks.InvocationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class AutoScheduledProcessorTrigger implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AutoScheduledProcessorTrigger.class);

    private static final int MAX_ITERATIONS_PER_TRIGGER = 500000;
    private static final int MAX_ITERATIONS_SOURCE_PROCESSOR = 500;

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
        final List<ProcessorTriggerContext> triggerContexts = taskQueue.getTriggerContexts();

        boolean triggered = false;
        for (final ProcessorTriggerContext triggerContext : triggerContexts) {
            try {
                if (isTrigger(triggerContext)) {
                    final long maxIterations;
                    if (triggerContext.isSourceComponent()) {
                        maxIterations = MAX_ITERATIONS_SOURCE_PROCESSOR;
                    } else {
                        maxIterations = MAX_ITERATIONS_PER_TRIGGER;
                    }

                    if (triggerContext.isBatchSupported()) {
                        final InvocationResult result = triggerContext.invoke(triggerContext.getRunDurationNanos(), maxIterations);

                        if (result.isYield()) {
                            triggerContext.getProcessor().yield(boredYieldMillis, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        triggerForDuration(triggerContext, triggerContext.getRunDurationNanos(), maxIterations);
                    }

                    triggered = true;
                }
            } catch (final Exception e) {
                logger.error("Failed to trigger {}", triggerContext.getProcessor(), e);
            }
        }

        return triggered;
    }

    private void triggerForDuration(final ProcessorTriggerContext triggerContext, final long runDuration, final long maxIterations) {
        final long stopTime = System.nanoTime() + runDuration;
        long count = 0L;
        while (count++ < maxIterations && System.nanoTime() < stopTime) {
            final InvocationResult result = triggerContext.invoke();
            if (result.isYield()) {
                triggerContext.getProcessor().yield(boredYieldMillis, TimeUnit.MILLISECONDS);
                return;
            }
        }
    }

    private boolean isTrigger(final ProcessorTriggerContext triggerContext) {
        if (true) {
            return true;
        }

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
        if (activeThreadCount >= triggerContext.getMaxConcurrentTasks()) {
            return false;
        }

        if (!triggerContext.isWorkToDo()) {
            return false;
        }

        return true;
    }
}
