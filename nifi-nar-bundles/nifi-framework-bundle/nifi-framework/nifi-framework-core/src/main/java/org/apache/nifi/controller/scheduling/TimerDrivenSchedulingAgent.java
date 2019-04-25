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
import org.apache.nifi.controller.tasks.DelayedInvocation;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TimerDrivenSchedulingAgent implements SchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(TimerDrivenSchedulingAgent.class);
    private final long noWorkYieldNanos;
    private final FlowEngine flowEngine;

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final StringEncryptor encryptor;

    private volatile String adminYieldDuration = "1 sec";
    private final DelayQueue<DelayedTriggerTask> taskQueue = new DelayQueue<>();

    private int maxThreads = 0;

    // TODO: Ensure this is done: Must separate out any other tasks into a different executor because this will use all of the threads in the Timer-Driven thread pool.

    public TimerDrivenSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final RepositoryContextFactory contextFactory,
            final StringEncryptor encryptor, final NiFiProperties nifiProperties) {
        this.flowEngine = flowEngine;
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.encryptor = encryptor;

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new RuntimeException("Failed to create SchedulingAgent because the " + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration);
        }
    }

    @Override
    public void shutdown() {
        flowEngine.shutdown();
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode, final LifecycleState lifecycleState) {
        final ReportingTaskWrapper reportingTaskWrapper = new ReportingTaskWrapper(taskNode, lifecycleState, flowController.getExtensionManager());

        final ScheduleCalculator scheduleCalculator = createScheduleCalculator(taskNode, taskNode.getSchedulingStrategy(), taskNode.getSchedulingPeriod(),
            taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS));

        final ReportingTaskDelayedInvocation invocation = new ReportingTaskDelayedInvocation(reportingTaskWrapper, scheduleCalculator);
        final DelayedTriggerTask task = new DelayedTriggerTask(taskNode, invocation, scheduleCalculator.getInitialTriggerTime());

        final boolean added = taskQueue.add(task);
        if (!added) {
            // Should never happen because the queue is unbounded.
            throw new IllegalStateException("Failed to schedule " + task + " to run because was unable to add its task to the queue");
        }

        lifecycleState.setComponentTasks(Collections.singleton(task));
        logger.info("{} started.", taskNode.getReportingTask());
    }

    @Override
    public void schedule(final Connectable connectable, final LifecycleState lifecycleState) {
        final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState, encryptor);

        final ScheduleCalculator scheduleCalculator = createScheduleCalculator(connectable, connectable.getSchedulingStrategy(), connectable.getSchedulingPeriod(),
            connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS));

        final Set<ComponentTask> componentTasks = new HashSet<>();
        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            final ConnectableDelayedInvocation invocation = new ConnectableDelayedInvocation(connectableTask, noWorkYieldNanos, scheduleCalculator);
            final DelayedTriggerTask task = new DelayedTriggerTask(connectable, invocation, scheduleCalculator.getInitialTriggerTime());
            componentTasks.add(task);

            final boolean added = taskQueue.add(task);
            if (!added) {
                // Should never happen because the queue is unbounded.
                throw new IllegalStateException("Failed to schedule " + connectable + " to run because was unable to add its task to the queue");
            }
        }

        lifecycleState.setComponentTasks(componentTasks);
        logger.info("Scheduled {} to run with {} threads", connectable, connectable.getMaxConcurrentTasks());
    }

    private ScheduleCalculator createScheduleCalculator(final Object component, final SchedulingStrategy schedulingStrategy, final String schedulingPeriod, final long schedulingNanos) {
        switch (schedulingStrategy) {
            case TIMER_DRIVEN:
            case PRIMARY_NODE_ONLY:
                return new PeriodicScheduleCalculator(0L, schedulingNanos);
            case CRON_DRIVEN:
                try {
                    return new CronScheduleCalculator(schedulingPeriod);
                } catch (final Exception pe) {
                    throw new IllegalStateException("Cannot schedule " + component + " to run because its scheduling period is not valid");
                }
        }

        return null;
    }


    @Override
    public void unschedule(final Connectable connectable, final LifecycleState scheduleState) {
        // stop scheduling to run but do not interrupt currently running tasks.
        scheduleState.getComponentTasks().forEach(ComponentTask::cancel);

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        // stop scheduling to run but do not interrupt currently running tasks.
        scheduleState.getComponentTasks().forEach(ComponentTask::cancel);

        logger.info("Stopped scheduling {} to run", taskNode.getReportingTask());
    }

    @Override
    public void setAdministrativeYieldDuration(final String yieldDuration) {
        this.adminYieldDuration = yieldDuration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(adminYieldDuration, timeUnit);
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public synchronized void setMaxThreadCount(final int maxThreadCount) {
        if (maxThreadCount > this.maxThreads) {
            final int added = maxThreadCount - this.maxThreads;
            for (int i=0; i < added; i++) {
                addRunComponentTask();
            }
        }

        this.maxThreads = maxThreadCount;
    }

    @Override
    public void incrementMaxThreadCount(int toAdd) {
        final int corePoolSize = flowEngine.getCorePoolSize();
        if (toAdd < 0 && corePoolSize + toAdd < 1) {
            throw new IllegalStateException("Cannot remove " + (-toAdd) + " threads from pool because there are only " + corePoolSize + " threads in the pool");
        }

        flowEngine.setCorePoolSize(corePoolSize + toAdd);

        for (int i=0; i < toAdd; i++) {
            addRunComponentTask();
        }
    }

    private void addRunComponentTask() {
        final RunComponentTask task = new RunComponentTask();
        flowEngine.submit(task);
    }


    private class DelayedTriggerTask implements Delayed, ComponentTask {
        private volatile long nextTriggerTime;
        private volatile boolean canceled = false;

        private final Object component;
        private final DelayedInvocation invocation;
        private final AtomicLong invocationCount;
        private final long maxInvocationCount;

        public DelayedTriggerTask(final Object component, final DelayedInvocation invocation, final long firstTriggerTimeNanos) {
            this(component, invocation, firstTriggerTimeNanos, new AtomicLong(0L), -1L);
        }

        public DelayedTriggerTask(final Object component, final DelayedInvocation invocation, final long firstTriggerTimeNanos, final AtomicLong invocationCounter, final long maxInvocationCount) {
            this.component = component;
            this.invocation = invocation;
            this.nextTriggerTime = firstTriggerTimeNanos;

            this.invocationCount = invocationCounter;
            this.maxInvocationCount = maxInvocationCount;
        }

        @Override
        public void cancel() {
            canceled = true;
        }

        public boolean isCanceled() {
            return canceled || (maxInvocationCount > 0 && invocationCount.get() >= maxInvocationCount);
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            final long now = System.nanoTime();
            final long delayNanos = nextTriggerTime - now;
            return unit.convert(delayNanos, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(final Delayed delayed) {
            if (delayed instanceof DelayedTriggerTask) {
                final DelayedTriggerTask other = (DelayedTriggerTask) delayed;
                return Long.compare(this.nextTriggerTime, other.nextTriggerTime);
            }

            return 0;
        }

        public void invoke() {
            if (maxInvocationCount > 0L ) {
                final long invocations = invocationCount.incrementAndGet();
                if (invocations > maxInvocationCount) {
                    logger.debug("Invocation triggered for {} but will not trigger component because it's been invoked {} times, which meets or exceeds its max invocations of {}",
                        component, invocations, maxInvocationCount);

                    return;
                }
            }

            this.nextTriggerTime = invocation.invoke();
        }

        @Override
        public String toString() {
            return "DelayedComponentTask[component=" + component + ", Delay=" + getDelay(TimeUnit.NANOSECONDS) + " ns]";
        }

        @Override
        public int hashCode() {
            return 17 + 13 * component.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            return obj == this;
        }
    }


    private class RunComponentTask implements Runnable {
        @Override
        public void run() {
            while (!flowEngine.isShutdown()) {
                try {
                    final DelayedTriggerTask task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task == null) {
                        logger.debug("No task to run");
                        continue;
                    }

                    if (task.isCanceled()) {
                        logger.debug("Will not invoke canceled task {}", task);
                        continue;
                    }

                    logger.debug("Triggering task {}", task);
                    task.invoke();

                    final boolean added = taskQueue.offer(task);
                    if (!added) {
                        logger.error("Failed to add component task {} to task queue", task);
                    }
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (final Throwable t) {
                    logger.error("Failed to trigger component task", t);
                }
            }
        }
    }
}
