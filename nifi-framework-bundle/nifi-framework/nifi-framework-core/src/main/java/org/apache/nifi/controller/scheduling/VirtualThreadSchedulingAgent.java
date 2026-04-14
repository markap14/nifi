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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Scheduling agent that uses virtual threads instead of a shared {@link FlowEngine} thread pool
 * for processor trigger loops. Global concurrency is bounded by a {@link DynamicSemaphore} whose
 * permit count replaces the old FlowEngine core pool size.
 * <p>
 * For AUTO-strategy processors, this agent delegates scaling decisions to a
 * {@link VirtualThreadScaler} chain (queue-based heuristics wrapped by a CPU-load guard).
 */
public class VirtualThreadSchedulingAgent extends AbstractSchedulingAgent {
    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadSchedulingAgent.class);
    private static final long POLL_INTERVAL_MILLIS = 25L;

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final DynamicSemaphore globalSemaphore;
    private final long noWorkYieldNanos;
    private final int systemMaxConcurrentTasks;
    private final VirtualThreadScaler scaler;
    private volatile String adminYieldDuration = "1 sec";
    private final Map<Connectable, ScalingState> scalingStates = new ConcurrentHashMap<>();

    public VirtualThreadSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine,
                                        final RepositoryContextFactory contextFactory, final NiFiProperties nifiProperties,
                                        final int maxThreadCount) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.globalSemaphore = new DynamicSemaphore(maxThreadCount);
        this.systemMaxConcurrentTasks = nifiProperties.getMaxConcurrentTasks();

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new RuntimeException("Failed to create VirtualThreadSchedulingAgent because the "
                    + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration);
        }

        final QueueBasedScaler queueBasedScaler = new QueueBasedScaler(flowController.getFlowFileEventRepository());
        this.scaler = new CpuLoadScaler(queueBasedScaler);
    }

    VirtualThreadSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine,
                                 final RepositoryContextFactory contextFactory, final NiFiProperties nifiProperties,
                                 final int maxThreadCount, final VirtualThreadScaler scaler) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.globalSemaphore = new DynamicSemaphore(maxThreadCount);
        this.systemMaxConcurrentTasks = nifiProperties.getMaxConcurrentTasks();

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new RuntimeException("Failed to create VirtualThreadSchedulingAgent because the "
                    + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration);
        }

        this.scaler = scaler;
    }

    @Override
    public void shutdown() {
    }

    @Override
    protected void doSchedule(final Connectable connectable, final LifecycleState scheduleState) {
        final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, scheduleState);
        final boolean autoMode = connectable.getSchedulingStrategy() == SchedulingStrategy.AUTO;

        if (autoMode) {
            final ScalingState scalingState = new ScalingState(systemMaxConcurrentTasks);
            scalingStates.put(connectable, scalingState);

            final String threadName = buildThreadName(connectable, 0);
            Thread.ofVirtual().name(threadName).start(
                    () -> runAutoSchedulingLoop(connectable, connectableTask, scheduleState, scalingState, 0));
            logger.info("Scheduled {} in auto mode with 1 initial virtual thread (max {})", connectable, systemMaxConcurrentTasks);
        } else {
            final int configuredTasks = connectable.getMaxConcurrentTasks();
            final int taskCount = Math.min(configuredTasks, systemMaxConcurrentTasks);
            for (int i = 0; i < taskCount; i++) {
                final int taskIndex = i;
                final String threadName = buildThreadName(connectable, taskIndex);
                Thread.ofVirtual().name(threadName).start(
                        () -> runFixedSchedulingLoop(connectable, connectableTask, scheduleState));
            }
            logger.info("Scheduled {} to run with {} virtual threads", connectable, taskCount);
        }

        scheduleState.setFutures(Collections.emptyList());
    }

    @Override
    protected void doScheduleOnce(final Connectable connectable, final LifecycleState scheduleState,
                                  final Callable<Future<Void>> stopCallback) {
        final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, scheduleState);
        final String threadName = buildThreadName(connectable, 0);

        Thread.ofVirtual().name(threadName).start(() -> {
            try {
                globalSemaphore.acquire();
                try {
                    connectableTask.invoke();
                } finally {
                    globalSemaphore.release();
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                try {
                    stopCallback.call();
                } catch (final Exception e) {
                    logger.error("Error while stopping {} after running once", connectable, e);
                    throw new ProcessException("Error while stopping " + connectable + " after running once", e);
                }
            }
        });

        scheduleState.setFutures(Collections.emptyList());
    }

    @Override
    protected void doUnschedule(final Connectable connectable, final LifecycleState scheduleState) {
        scalingStates.remove(connectable);
        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    protected void doSchedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, scheduleState, flowController.getExtensionManager());
        final long schedulingNanos = taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        final String threadName = "Reporting Task: " + taskNode.getName();

        Thread.ofVirtual().name(threadName).start(() -> {
            while (scheduleState.isScheduled()) {
                reportingTaskWrapper.run();
                sleepWithPolling(schedulingNanos, scheduleState);
            }
        });

        scheduleState.setFutures(Collections.emptyList());
        logger.info("{} started on virtual thread", taskNode.getReportingTask());
    }

    @Override
    protected void doUnschedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        logger.info("Stopped scheduling {} to run", taskNode.getReportingTask());
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public void setMaxThreadCount(final int maxThreads) {
        globalSemaphore.setMaxPermits(maxThreads);
        logger.info("Global semaphore permits updated to {}", maxThreads);
    }

    @Override
    public void incrementMaxThreadCount(final int toAdd) {
    }

    @Override
    public void setAdministrativeYieldDuration(final String duration) {
        this.adminYieldDuration = duration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(adminYieldDuration, timeUnit);
    }

    DynamicSemaphore getGlobalSemaphore() {
        return globalSemaphore;
    }

    /**
     * Returns the current target concurrent tasks for an auto-mode processor,
     * or -1 if the processor is not in auto mode.
     */
    public int getTargetConcurrentTasks(final Connectable connectable) {
        final ScalingState scalingState = scalingStates.get(connectable);
        return scalingState != null ? scalingState.getTargetConcurrency().get() : -1;
    }

    private void runFixedSchedulingLoop(final Connectable connectable, final ConnectableTask connectableTask,
                                        final LifecycleState lifecycleState) {
        final boolean cronDriven = connectable.getSchedulingStrategy() == SchedulingStrategy.CRON_DRIVEN;

        CronExpression cronExpression = null;
        OffsetDateTime nextCronSchedule = null;
        if (cronDriven) {
            final String cronSchedule = connectable.evaluateParameters(connectable.getSchedulingPeriod());
            cronExpression = CronExpression.parse(cronSchedule);
            nextCronSchedule = getNextCronSchedule(OffsetDateTime.now(), cronExpression);
            final long initialDelay = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
            if (initialDelay > 0 && lifecycleState.isScheduled()) {
                sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(initialDelay), lifecycleState);
            }
        }

        while (lifecycleState.isScheduled()) {
            InvocationResult invocationResult;
            try {
                globalSemaphore.acquire();
                try {
                    invocationResult = connectableTask.invoke();
                } finally {
                    globalSemaphore.release();
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (cronDriven) {
                nextCronSchedule = getNextCronSchedule(nextCronSchedule, cronExpression);
                final long sleepMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
                sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(sleepMillis), lifecycleState);
            } else {
                sleepForSchedulingPeriod(connectable, lifecycleState, invocationResult);
            }
        }
    }

    private void runAutoSchedulingLoop(final Connectable connectable, final ConnectableTask connectableTask,
                                       final LifecycleState lifecycleState, final ScalingState scalingState,
                                       final int taskIndex) {
        while (lifecycleState.isScheduled() && taskIndex < scalingState.getTargetConcurrency().get()) {
            InvocationResult invocationResult;
            try {
                globalSemaphore.acquire();
                try {
                    invocationResult = connectableTask.invoke();
                } finally {
                    globalSemaphore.release();
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            final InvocationResult.YieldReason yieldReason = invocationResult.getYieldReason();
            if (yieldReason == InvocationResult.YieldReason.TERMINATED) {
                return;
            } else if (invocationResult.isYield() || yieldReason == InvocationResult.YieldReason.YIELDED) {
                sleepForYieldOrNoWork(connectable, lifecycleState);
            } else {
                evaluateScalingIfDue(connectable, scalingState, connectableTask, lifecycleState);
            }
        }
    }

    private void sleepForYieldOrNoWork(final Connectable connectable, final LifecycleState lifecycleState) {
        final long yieldExpiration = connectable.getYieldExpiration();
        final long sleepMillis;
        if (yieldExpiration > System.currentTimeMillis()) {
            sleepMillis = yieldExpiration - System.currentTimeMillis();
        } else {
            sleepMillis = TimeUnit.NANOSECONDS.toMillis(noWorkYieldNanos);
        }
        sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(sleepMillis), lifecycleState);
    }

    // Multiple virtual threads for the same processor may concurrently observe isEvaluationDue() == true
    // because the check-then-set on lastEvaluationTime is not atomic. This is benign: targetConcurrency
    // is an AtomicInteger, so concurrent increments/decrements are safe. In the worst case, two threads
    // may both evaluate and produce a slightly more aggressive scaling adjustment than a single evaluation
    // would, which self-corrects on the next evaluation cycle.
    private void evaluateScalingIfDue(final Connectable connectable, final ScalingState scalingState,
                                      final ConnectableTask connectableTask, final LifecycleState lifecycleState) {
        if (!scalingState.isEvaluationDue()) {
            return;
        }
        scalingState.setLastEvaluationTime(System.currentTimeMillis());

        final ScalingRecommendation recommendation = scaler.evaluate(connectable, scalingState);
        switch (recommendation) {
            case SCALE_DOWN:
                final int previousDown = scalingState.getTargetConcurrency().getAndUpdate(c -> c > 1 ? c - 1 : c);
                if (previousDown > 1) {
                    logger.debug("Decreasing {}'s effective threads from {} to {}", connectable.getName(), previousDown, previousDown - 1);
                }
                break;
            case SCALE_UP:
                scalingState.setPendingValidation(true);
                final int newTarget = scalingState.getTargetConcurrency().incrementAndGet();
                logger.debug("Increasing {}'s effective threads from {} to {}", connectable.getName(), newTarget - 1, newTarget);
                final String threadName = buildThreadName(connectable, newTarget - 1);
                Thread.ofVirtual().name(threadName).start(
                        () -> runAutoSchedulingLoop(connectable, connectableTask, lifecycleState, scalingState, newTarget - 1));
                break;
            case HOLD:
            default:
                break;
        }
    }

    private void sleepForSchedulingPeriod(final Connectable connectable, final LifecycleState lifecycleState,
                                          final InvocationResult invocationResult) {
        final long sleepMillis;
        final long yieldExpiration = connectable.getYieldExpiration();
        if (yieldExpiration > System.currentTimeMillis()) {
            sleepMillis = yieldExpiration - System.currentTimeMillis();
        } else if (invocationResult.isYield()) {
            sleepMillis = TimeUnit.NANOSECONDS.toMillis(noWorkYieldNanos);
        } else {
            sleepMillis = connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        }

        sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(sleepMillis), lifecycleState);
    }

    private void sleepWithPolling(final long sleepNanos, final LifecycleState lifecycleState) {
        final long sleepExpiration = System.nanoTime() + sleepNanos;
        while (System.nanoTime() < sleepExpiration && lifecycleState.isScheduled()) {
            try {
                Thread.sleep(POLL_INTERVAL_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static String buildThreadName(final Connectable connectable, final int taskIndex) {
        return connectable.getName() + " [id=" + connectable.getIdentifier() + ", group=" + connectable.getProcessGroup().getName() + "] " + taskIndex;
    }

    private static OffsetDateTime getNextCronSchedule(final OffsetDateTime currentSchedule, final CronExpression cronExpression) {
        final OffsetDateTime now = OffsetDateTime.now();
        return cronExpression.next(now.isAfter(currentSchedule) ? now : currentSchedule);
    }
}
