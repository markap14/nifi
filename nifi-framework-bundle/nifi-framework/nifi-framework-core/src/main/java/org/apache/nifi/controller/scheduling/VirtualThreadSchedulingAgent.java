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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Triggerable;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSchedulingRegistration;
import org.apache.nifi.controller.scheduling.auto.AutoControllerPhase;
import org.apache.nifi.controller.scheduling.auto.AutoDecisionReason;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingDecision;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingDiagnostics;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingMetrics;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingObservation;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingResetReason;
import org.apache.nifi.controller.scheduling.auto.NodeSchedulingObservation;
import org.apache.nifi.controller.scheduling.auto.NodeSchedulingObservationSampler;
import org.apache.nifi.controller.scheduling.auto.StandardAutoSchedulingController;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationObserver;
import org.apache.nifi.controller.tasks.InvocationOutcome;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.Connectables;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Scheduling agent that runs components on virtual threads. A {@link DynamicSemaphore}
 * limits the number of component invocations that can run concurrently.
 */
public class VirtualThreadSchedulingAgent implements SchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadSchedulingAgent.class);

    private static final long PERMIT_POLL_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final DynamicSemaphore globalSemaphore;
    private final int autoMaxConcurrentTasks;
    private final long noWorkYieldNanos;
    private final ExecutorService executorService;
    private final ScheduledExecutorService controlExecutor;
    private final NodeSchedulingObservationSampler nodeObservationSampler;
    private final ConcurrentMap<String, SchedulingGeneration> schedulingGenerations = new ConcurrentHashMap<>();
    private final LongAdder globalPermitHoldNanos = new LongAdder();
    private final LongAdder globalPermitWaitNanos = new LongAdder();
    private final LongAdder globalFullOccupancySamples = new LongAdder();
    private final LongAdder globalOccupancySamples = new LongAdder();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final AtomicInteger runningThreadCount = new AtomicInteger();
    private volatile NodeSchedulingObservation nodeSchedulingObservation = new NodeSchedulingObservation(false, -1D, -1D, 0D, 1, 0D, 0D, false);
    private volatile long lastNodeObservationNanos = System.nanoTime();
    private volatile String adminYieldDuration = "1 sec";
    private volatile long adminYieldNanos = TimeUnit.SECONDS.toNanos(1L);

    public VirtualThreadSchedulingAgent(final FlowController flowController, final RepositoryContextFactory contextFactory,
                                        final NiFiProperties nifiProperties, final int maxThreadCount) {
        this(flowController, contextFactory, nifiProperties, maxThreadCount, nifiProperties.getProcessorAutoMaxConcurrentTasks());
    }

    public VirtualThreadSchedulingAgent(final FlowController flowController, final RepositoryContextFactory contextFactory,
                                        final NiFiProperties nifiProperties, final int maxThreadCount, final int autoMaxConcurrentTasks) {
        this(flowController, contextFactory, nifiProperties, maxThreadCount, autoMaxConcurrentTasks, new NodeSchedulingObservationSampler());
    }

    VirtualThreadSchedulingAgent(final FlowController flowController, final RepositoryContextFactory contextFactory, final NiFiProperties nifiProperties,
                                final int maxThreadCount, final int autoMaxConcurrentTasks, final NodeSchedulingObservationSampler nodeObservationSampler) {
        this.nodeObservationSampler = nodeObservationSampler;
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.globalSemaphore = new DynamicSemaphore(maxThreadCount);
        this.autoMaxConcurrentTasks = autoMaxConcurrentTasks;

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException("Failed to create VirtualThreadSchedulingAgent because the "
                    + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration, e);
        }

        final ThreadFactory threadFactory = runnable -> {
            final Thread thread = Thread.ofVirtual().inheritInheritableThreadLocals(false).unstarted(runnable);
            thread.setContextClassLoader(NarThreadContextClassLoader.getInstance());
            return thread;
        };
        executorService = Executors.newThreadPerTaskExecutor(threadFactory);
        controlExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = Thread.ofPlatform().daemon(true).name("Automatic Processor Scheduling Controller").unstarted(runnable);
            thread.setContextClassLoader(NarThreadContextClassLoader.getInstance());
            return thread;
        });
        controlExecutor.scheduleWithFixedDelay(this::evaluateAutoScheduling, 250L, 250L, TimeUnit.MILLISECONDS);
        logger.info("VirtualThreadSchedulingAgent initialized with {} permits", maxThreadCount);
    }

    @Override
    public int getProcessContextConcurrencyLimit(final Connectable connectable) {
        if (connectable.getSchedulingStrategy() != SchedulingStrategy.AUTO) {
            return connectable.getMaxConcurrentTasks();
        }

        return connectable instanceof ProcessorNode processorNode && processorNode.isTriggeredSerially() ? 1 : autoMaxConcurrentTasks;
    }

    @Override
    public void shutdown() {
        signalShutdown(true);
        controlExecutor.shutdownNow();
        executorService.shutdownNow();
    }

    public void shutdownGracefully() {
        signalShutdown(false);
        controlExecutor.shutdown();
        executorService.shutdown();
    }

    private void signalShutdown(final boolean interrupt) {
        shutdown.set(true);

        for (final SchedulingGeneration generation : schedulingGenerations.values()) {
            generation.stop(interrupt);
        }
    }

    public boolean awaitTermination(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + timeUnit.toNanos(timeout);
        if (!controlExecutor.awaitTermination(timeout, timeUnit)) {
            return false;
        }

        final long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        return executorService.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
    }

    public boolean isTerminated() {
        return controlExecutor.isTerminated() && executorService.isTerminated();
    }

    @Override
    public void schedule(final Connectable connectable, final LifecycleState lifecycleState) {
        final boolean cronDriven = connectable.getSchedulingStrategy() == SchedulingStrategy.CRON_DRIVEN;
        final CronExpression cronExpression;
        final long schedulingNanos;
        if (cronDriven) {
            final String cronSchedule = connectable.evaluateParameters(connectable.getSchedulingPeriod());
            cronExpression = parseCronExpression(cronSchedule, connectable);
            schedulingNanos = 0L;
        } else {
            cronExpression = null;
            schedulingNanos = connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        }

        final String componentId = connectable.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState);
            if (connectable.getSchedulingStrategy() == SchedulingStrategy.AUTO) {
                final int contextCeiling = getProcessContextConcurrencyLimit(connectable);
                final AutoSchedulingState autoSchedulingState = new AutoSchedulingState(connectable, connectableTask, lifecycleState, generation,
                        contextCeiling, connectable.isSessionBatchingSupported(), connectable instanceof ProcessorNode processorNode && processorNode.isTriggeredSerially());
                generation.setAutoSchedulingState(autoSchedulingState);
                registerQueueListeners(connectable, generation);
                startAutoWorkers(autoSchedulingState, 1);
                logger.info("Scheduled {} for automatic scheduling with a context ceiling of {}", connectable, contextCeiling);
                return;
            }

            final int taskCount = connectable.getMaxConcurrentTasks();
            for (int i = 0; i < taskCount; i++) {
                final String threadName = buildThreadName(connectable, i);
                submitTask(threadName, generation, () -> runSchedulingLoop(connectable, connectableTask, schedulingNanos, lifecycleState, generation, cronExpression));
            }

            logger.info("Scheduled {} to run with {} virtual threads", connectable, taskCount);
        } catch (final Throwable t) {
            synchronized (lifecycleState) {
                if (stopSchedulingGeneration(componentId, generation, true)) {
                    lifecycleState.setScheduled(false);
                }
            }

            throw t;
        }
    }

    private void registerQueueListeners(final Connectable connectable, final SchedulingGeneration generation) {
        final Set<FlowFileQueue> queues = new HashSet<>();
        for (final Connection connection : connectable.getIncomingConnections()) {
            queues.add(connection.getFlowFileQueue());
        }

        for (final Connection connection : connectable.getConnections()) {
            queues.add(connection.getFlowFileQueue());
        }

        for (final FlowFileQueue queue : queues) {
            generation.addQueue(queue);
            generation.addQueueRegistration(queue.addSchedulingListener(generation::signalChange));
        }
    }

    private void startAutoWorkers(final AutoSchedulingState state, final int desiredWorkerCount) {
        final List<AutoWorker> workers = state.resizeWorkers(desiredWorkerCount);
        for (final AutoWorker worker : workers) {
            final String threadName = buildThreadName(state.connectable, Math.toIntExact(worker.identifier));
            submitTask(threadName, state.generation, () -> runAutoSchedulingLoop(state, worker));
        }
    }

    private void runAutoSchedulingLoop(final AutoSchedulingState state, final AutoWorker worker) {
        try {
            while (isActive(state.lifecycleState, state.generation) && !worker.retired.get()) {
                if (!state.connectableTask.isReady() || !state.isSourceProbeEligible()) {
                    waitForAutoReadiness(state, worker);
                    continue;
                }

                final long admissionChangeSequence = state.generation.getAdmissionChangeSequence();
                if (!acquirePermitWithPolling(state.lifecycleState, state.generation)) {
                    return;
                }

                final SchedulingSettings policy = state.policy.get();
                final long permitHoldStartNanos = System.nanoTime();
                boolean sourceProbeAcquired = false;
                boolean workerAdmitted = false;
                boolean invocationAttempted = false;
                boolean workerAdmissionDenied = false;
                try {
                    if (!isActive(state.lifecycleState, state.generation) || worker.retired.get() || !state.connectableTask.isReady()) {
                        continue;
                    }

                    workerAdmitted = state.tryAdmitWorker(policy);
                    if (workerAdmitted) {
                        sourceProbeAcquired = state.acquireSourceProbe();
                        if (state.requiresSourceProbe() && !sourceProbeAcquired) {
                            continue;
                        }

                        invocationAttempted = true;
                        final InvocationObserver observer = state.metrics.createObserver();
                        final InvocationResult result = state.connectableTask.invoke(policy.runDurationNanos(),
                                () -> isActive(state.lifecycleState, state.generation) && !worker.retired.get() && state.policy.get() == policy, observer);
                        state.recordInvocationResult(result);
                    } else {
                        workerAdmissionDenied = true;
                    }
                } finally {
                    if (workerAdmitted) {
                        state.releaseWorkerAdmission();
                    }

                    if (sourceProbeAcquired) {
                        state.releaseSourceProbe();
                    }

                    final long permitHoldNanos = System.nanoTime() - permitHoldStartNanos;
                    if (invocationAttempted) {
                        state.metrics.recordInvocationDuration(permitHoldNanos);
                    }

                    globalPermitHoldNanos.add(permitHoldNanos);
                    Thread.interrupted();
                    globalSemaphore.release();
                }

                if (workerAdmissionDenied) {
                    state.generation.awaitAdmissionChange(admissionChangeSequence, worker);
                }
            }
        } finally {
            state.workerStopped(worker);
        }
    }

    private void waitForAutoReadiness(final AutoSchedulingState state, final AutoWorker worker) {
        final long changeSequence = state.generation.getChangeSequence();
        if (state.connectableTask.isReady() && state.isSourceProbeEligible()) {
            return;
        }

        final long nowMillis = System.currentTimeMillis();
        long delayNanos = TimeUnit.SECONDS.toNanos(1L);
        final long queueDeadlineMillis = state.getNextQueueDeadlineMillis();
        if (queueDeadlineMillis > nowMillis) {
            delayNanos = Math.min(delayNanos, TimeUnit.MILLISECONDS.toNanos(queueDeadlineMillis - nowMillis));
        }

        final long sourceDelayNanos = state.getSourceProbeDelayNanos();
        if (sourceDelayNanos > 0L) {
            delayNanos = Math.min(delayNanos, sourceDelayNanos);
        }

        state.generation.awaitChange(changeSequence, delayNanos, worker);
    }

    @Override
    public void scheduleOnce(final Connectable connectable, final LifecycleState lifecycleState, final Callable<Future<Void>> stopCallback) {
        final String componentId = connectable.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState);
            final String threadName = buildThreadName(connectable, 0);

            submitTask(threadName, generation, () -> {
                try {
                    runOnce(connectable, connectableTask, stopCallback, lifecycleState, generation);
                } finally {
                    stopSchedulingGeneration(componentId, generation, false);
                }
            });
        } catch (final Throwable t) {
            synchronized (lifecycleState) {
                if (stopSchedulingGeneration(componentId, generation, true)) {
                    lifecycleState.setScheduled(false);
                }
            }

            throw t;
        }
    }

    @Override
    public void unschedule(final Connectable connectable, final LifecycleState lifecycleState) {
        synchronized (lifecycleState) {
            final SchedulingGeneration generation = schedulingGenerations.remove(connectable.getIdentifier());
            if (generation != null) {
                generation.stop(false);
            }

            lifecycleState.setScheduled(false);
        }

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode, final LifecycleState lifecycleState) {
        final boolean cronDriven = taskNode.getSchedulingStrategy() == SchedulingStrategy.CRON_DRIVEN;
        final CronExpression cronExpression;
        final long schedulingNanos;
        if (cronDriven) {
            cronExpression = parseCronExpression(taskNode.getSchedulingPeriod(), taskNode);
            schedulingNanos = 0L;
        } else {
            cronExpression = null;
            schedulingNanos = taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        }

        final String componentId = taskNode.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, lifecycleState, flowController.getExtensionManager());
            final String threadName = "Reporting Task: " + taskNode.getName();

            submitTask(threadName, generation,
                    () -> runReportingTaskLoop(taskNode, reportingTaskWrapper, schedulingNanos, cronExpression, lifecycleState, generation));

            logger.info("{} started on virtual thread", taskNode.getReportingTask());
        } catch (final Throwable t) {
            synchronized (lifecycleState) {
                if (stopSchedulingGeneration(componentId, generation, true)) {
                    lifecycleState.setScheduled(false);
                }
            }

            throw t;
        }
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode, final LifecycleState lifecycleState) {
        synchronized (lifecycleState) {
            final SchedulingGeneration generation = schedulingGenerations.remove(taskNode.getIdentifier());
            if (generation != null) {
                generation.stop(false);
            }

            lifecycleState.setScheduled(false);
        }

        logger.info("Stopped scheduling {} to run", taskNode.getReportingTask());
    }

    private SchedulingGeneration registerSchedulingGeneration(final String componentId) {
        if (shutdown.get()) {
            throw new IllegalStateException("VirtualThreadSchedulingAgent has been shut down and cannot accept new work");
        }

        final SchedulingGeneration generation = new SchedulingGeneration();
        final SchedulingGeneration existingGeneration = schedulingGenerations.putIfAbsent(componentId, generation);
        if (existingGeneration != null) {
            throw new IllegalStateException("Component " + componentId + " is already scheduled");
        }

        if (shutdown.get()) {
            stopSchedulingGeneration(componentId, generation, true);
            throw new IllegalStateException("VirtualThreadSchedulingAgent has been shut down and cannot accept new work");
        }

        return generation;
    }

    private boolean stopSchedulingGeneration(final String componentId, final SchedulingGeneration generation, final boolean interrupt) {
        final boolean removed = schedulingGenerations.remove(componentId, generation);
        generation.stop(interrupt);
        return removed;
    }

    private boolean isActive(final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        return !shutdown.get() && lifecycleState.isScheduled() && !generation.isStopped();
    }

    private void evaluateAutoScheduling() {
        try {
            final long nowNanos = System.nanoTime();
            final int currentGlobalBudget = globalSemaphore.getMaxPermits();
            globalOccupancySamples.increment();
            if (globalSemaphore.getInUsePermits() >= currentGlobalBudget) {
                globalFullOccupancySamples.increment();
            }

            if (nowNanos - lastNodeObservationNanos >= TimeUnit.SECONDS.toNanos(1L)) {
                final int globalBudget = currentGlobalBudget;
                final long occupancySamples = globalOccupancySamples.sumThenReset();
                final double utilization = occupancySamples == 0L ? 0D : globalFullOccupancySamples.sumThenReset() / (double) occupancySamples;
                final long permitWaitNanos = globalPermitWaitNanos.sumThenReset();
                final long permitHoldNanos = globalPermitHoldNanos.sumThenReset();
                final double permitUseNanos = (double) permitWaitNanos + permitHoldNanos;
                final double contention = permitUseNanos == 0D ? 0D : permitWaitNanos / permitUseNanos;
                nodeSchedulingObservation = nodeObservationSampler.sample(globalBudget, utilization, contention);
                lastNodeObservationNanos = nowNanos;
            }

            for (final SchedulingGeneration generation : schedulingGenerations.values()) {
                final AutoSchedulingState state = generation.getAutoSchedulingState();
                if (state == null || generation.isStopped()) {
                    continue;
                }

                state.recordControlSample(nowNanos);
                startAutoWorkers(state, state.policy.get().concurrentTasks());
                if (!state.isEvaluationDue(nowNanos)) {
                    continue;
                }

                final AutoSchedulingObservation observation = state.snapshotObservation(nowNanos);
                final long resetSequence = state.resetSequence.get();
                final AutoSchedulingDecision decision = state.controller.evaluate(observation, nodeSchedulingObservation);
                logger.debug("Automatic scheduling evaluation for {}: observation={}, node={}, decision={}", state.connectable, observation, nodeSchedulingObservation, decision);
                if (generation.isStopped() || schedulingGenerations.get(state.connectable.getIdentifier()) != generation
                        || state.resetSequence.get() != resetSequence) {
                    continue;
                }

                state.recordDecision(decision);
                if (decision.applySettings()) {
                    final int selectedConcurrency = Math.max(1, Math.min(decision.settings().concurrentTasks(),
                            Math.min(state.contextCeiling, globalSemaphore.getMaxPermits())));
                    final SchedulingSettings settings = new SchedulingSettings(selectedConcurrency,
                            state.batchingSupported ? Math.min(decision.settings().runDurationNanos(), TimeUnit.MILLISECONDS.toNanos(25L)) : 0L);
                    state.applySettings(settings);
                    startAutoWorkers(state, selectedConcurrency);
                }
            }
        } catch (final Throwable failure) {
            logger.error("Failed to evaluate automatic Processor scheduling", failure);
        }
    }

    private static CronExpression parseCronExpression(final String cronSchedule, final Object component) {
        try {
            return CronExpression.parse(cronSchedule);
        } catch (final RuntimeException e) {
            throw new IllegalStateException("Cannot schedule " + component + " to run because its scheduling period is not a valid CRON expression: " + cronSchedule, e);
        }
    }

    @Override
    public void onEvent(final Connectable connectable) {
        final SchedulingGeneration generation = schedulingGenerations.get(connectable.getIdentifier());
        if (generation != null) {
            final AutoSchedulingState state = generation.getAutoSchedulingState();
            if (state != null) {
                requestControllerReset(generation, state, AutoSchedulingResetReason.TOPOLOGY_CHANGED);
            }

            generation.signalChange();
        }
    }

    @Override
    public synchronized void setMaxThreadCount(final int maxThreads) {
        globalSemaphore.setMaxPermits(maxThreads);
        resetAutoControllersForGlobalBudgetChange();
        logger.info("Global semaphore permits updated to {}", maxThreads);
    }

    @Override
    public synchronized void incrementMaxThreadCount(final int toAdd) {
        if (toAdd == 0) {
            return;
        }

        final int currentMax = globalSemaphore.getMaxPermits();
        final int newMax = currentMax + toAdd;
        if (newMax < 1) {
            throw new IllegalStateException("Cannot remove " + (-toAdd) + " permits from global semaphore because there are only " + currentMax + " permits available");
        }

        globalSemaphore.setMaxPermits(newMax);
        resetAutoControllersForGlobalBudgetChange();
    }

    private void resetAutoControllersForGlobalBudgetChange() {
        for (final SchedulingGeneration generation : schedulingGenerations.values()) {
            final AutoSchedulingState state = generation.getAutoSchedulingState();
            if (state != null) {
                requestControllerReset(generation, state, AutoSchedulingResetReason.GLOBAL_BUDGET_CHANGED);
                generation.signalChange();
            }
        }
    }

    private void requestControllerReset(final SchedulingGeneration generation, final AutoSchedulingState state, final AutoSchedulingResetReason reason) {
        final long resetSequence = state.resetSequence.incrementAndGet();
        try {
            controlExecutor.execute(() -> {
                if (generation.isStopped() || state.resetSequence.get() != resetSequence) {
                    return;
                }

                if (reason == AutoSchedulingResetReason.TOPOLOGY_CHANGED) {
                    generation.clearQueueRegistrations();
                    registerQueueListeners(state.connectable, generation);
                }

                state.resetMeasurementsAfterEvent();
                if (reason == AutoSchedulingResetReason.GLOBAL_BUDGET_CHANGED) {
                    final SchedulingSettings currentSettings = state.policy.get();
                    final int selectedConcurrency = Math.min(currentSettings.concurrentTasks(), globalSemaphore.getMaxPermits());
                    final SchedulingSettings clampedSettings = new SchedulingSettings(selectedConcurrency, currentSettings.runDurationNanos());
                    state.applySettings(clampedSettings);
                    startAutoWorkers(state, selectedConcurrency);
                }

                state.controller.reset();
                state.lastDecision = new AutoSchedulingDecision(state.policy.get(), AutoControllerPhase.HOLD, AutoDecisionReason.RESET, false);
            });
        } catch (final RejectedExecutionException e) {
            if (!shutdown.get()) {
                throw e;
            }
        }
    }

    @Override
    public void setAdministrativeYieldDuration(final String duration) {
        this.adminYieldNanos = FormatUtils.getTimeDuration(duration, TimeUnit.NANOSECONDS);
        this.adminYieldDuration = duration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return timeUnit.convert(adminYieldNanos, TimeUnit.NANOSECONDS);
    }

    DynamicSemaphore getGlobalSemaphore() {
        return globalSemaphore;
    }

    int getRunningThreadCount() {
        return runningThreadCount.get();
    }

    void requestAutoSchedulingSettings(final String componentIdentifier, final SchedulingSettings settings) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        if (generation == null || generation.getAutoSchedulingState() == null) {
            throw new IllegalStateException("Component " + componentIdentifier + " is not scheduled using automatic scheduling");
        }

        controlExecutor.execute(() -> {
            final AutoSchedulingState state = generation.getAutoSchedulingState();
            if (state == null || generation.isStopped() || schedulingGenerations.get(componentIdentifier) != generation) {
                return;
            }

            state.applySettings(settings);
            startAutoWorkers(state, settings.concurrentTasks());
        });
    }

    int getAutoSchedulingWaiterCount(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        return generation == null ? 0 : generation.getWaiterCount();
    }

    int getAutoSchedulingAdmissionWaiterCount(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        return generation == null ? 0 : generation.getAdmissionWaiterCount();
    }

    long getAutoSchedulingAdmissionSequence(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        return generation == null ? -1L : generation.getAdmissionChangeSequence();
    }

    boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * @return number of component invocations currently holding global permits
     */
    public int getActiveThreadCount() {
        return globalSemaphore.getInUsePermits();
    }

    public AutoSchedulingDiagnostics getAutoSchedulingDiagnostics(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        final AutoSchedulingState state = generation == null ? null : generation.getAutoSchedulingState();
        return state == null ? null : state.getDiagnostics();
    }

    private void runSchedulingLoop(final Connectable connectable, final ConnectableTask connectableTask, final long schedulingNanos,
                                   final LifecycleState lifecycleState, final SchedulingGeneration generation, final CronExpression cronExpression) {
        final boolean cronDriven = cronExpression != null;

        OffsetDateTime nextCronSchedule = null;
        if (cronDriven) {
            nextCronSchedule = getNextCronSchedule(OffsetDateTime.now(), cronExpression);
            if (nextCronSchedule == null) {
                logger.warn("CRON expression for {} has no future firings; scheduling loop will exit without invoking the component", connectable);
                return;
            }

            final long initialDelayMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
            if (initialDelayMillis > 0L) {
                waitForDelay(TimeUnit.MILLISECONDS.toNanos(initialDelayMillis), generation);
            }
        }

        while (true) {
            try {
                if (!acquirePermitWithPolling(lifecycleState, generation)) {
                    return;
                }

                final InvocationResult invocationResult;
                final long permitHoldStartNanos = System.nanoTime();
                try {
                    invocationResult = connectableTask.invoke();
                } finally {
                    // Interrupt status from one invocation must not carry into the scheduling loop.
                    Thread.interrupted();
                    globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
                    globalSemaphore.release();
                }

                if (cronDriven) {
                    nextCronSchedule = getNextCronSchedule(nextCronSchedule, cronExpression);
                    if (nextCronSchedule == null) {
                        logger.warn("CRON expression for {} has no further firings after the current invocation; scheduling loop is exiting", connectable);
                        return;
                    }

                    final long sleepMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
                    waitForDelay(TimeUnit.MILLISECONDS.toNanos(sleepMillis), generation);
                } else {
                    waitForNextInvocation(connectable, schedulingNanos, generation, invocationResult);
                }
            } catch (final Throwable t) {
                if (!isActive(lifecycleState, generation)) {
                    return;
                }

                try {
                    connectable.yield(adminYieldNanos, TimeUnit.NANOSECONDS);
                } catch (final Throwable yieldError) {
                    t.addSuppressed(yieldError);
                }

                logger.error("Unexpected error in scheduling loop for {}. Will yield for {} and continue.", connectable, adminYieldDuration, t);
                waitForDelay(adminYieldNanos, generation);
            }
        }
    }

    private void runOnce(final Connectable connectable, final ConnectableTask connectableTask, final Callable<Future<Void>> stopCallback,
                         final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        try {
            if (!acquirePermitWithPolling(lifecycleState, generation)) {
                if (isActive(lifecycleState, generation)) {
                    logger.warn("Run once request for {} was not executed because permit acquisition was interrupted", connectable);
                } else {
                    logger.warn("Run once request for {} was not executed because scheduling is no longer active", connectable);
                }

                return;
            }

            final long permitHoldStartNanos = System.nanoTime();
            try {
                connectableTask.invoke();
            } finally {
                globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
                globalSemaphore.release();
            }
        } catch (final Throwable t) {
            logger.error("Unexpected error running {} once", connectable, t);
        } finally {
            try {
                stopCallback.call();
            } catch (final Throwable t) {
                logger.error("Error while stopping {} after running once", connectable, t);
            }
        }
    }

    private void runReportingTaskLoop(final ReportingTaskNode taskNode, final Runnable reportingTaskWrapper, final long schedulingNanos,
                                      final CronExpression cronExpression, final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        final boolean cronDriven = cronExpression != null;

        OffsetDateTime nextCronSchedule = null;
        if (cronDriven) {
            nextCronSchedule = getNextCronSchedule(OffsetDateTime.now(), cronExpression);
            if (nextCronSchedule == null) {
                logger.warn("CRON expression for {} has no future firings; scheduling loop will exit without invoking the reporting task",
                        taskNode.getReportingTask());
                return;
            }

            final long initialDelayMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
            if (initialDelayMillis > 0L) {
                waitForDelay(TimeUnit.MILLISECONDS.toNanos(initialDelayMillis), generation);
            }
        }

        while (true) {
            try {
                if (!acquirePermitWithPolling(lifecycleState, generation)) {
                    return;
                }

                final long permitHoldStartNanos = System.nanoTime();
                try {
                    reportingTaskWrapper.run();
                } finally {
                    // Interrupt status from one invocation must not carry into the scheduling loop.
                    Thread.interrupted();
                    globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
                    globalSemaphore.release();
                }

                if (cronDriven) {
                    nextCronSchedule = getNextCronSchedule(nextCronSchedule, cronExpression);
                    if (nextCronSchedule == null) {
                        logger.warn("CRON expression for {} has no further firings after the current invocation; scheduling loop is exiting",
                                taskNode.getReportingTask());
                        return;
                    }

                    final long sleepMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
                    waitForDelay(TimeUnit.MILLISECONDS.toNanos(sleepMillis), generation);
                } else {
                    waitForDelay(schedulingNanos, generation);
                }
            } catch (final Throwable t) {
                if (!isActive(lifecycleState, generation)) {
                    return;
                }

                logger.error("Unexpected error in scheduling loop for {}. Will wait for {} and continue.", taskNode.getReportingTask(), adminYieldDuration, t);
                waitForDelay(adminYieldNanos, generation);
            }
        }
    }

    private void waitForNextInvocation(final Connectable connectable, final long schedulingNanos, final SchedulingGeneration generation,
                                       final InvocationResult invocationResult) {
        final long sleepNanos;
        final long yieldExpiration = connectable.getYieldExpiration();
        final long yieldDelayNanos;
        if (yieldExpiration == 0L) {
            yieldDelayNanos = 0L;
        } else {
            yieldDelayNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(yieldExpiration - System.currentTimeMillis(), 0L));
        }

        if (yieldDelayNanos > 0L) {
            sleepNanos = Math.max(schedulingNanos, yieldDelayNanos);
        } else if (invocationResult.isYield()) {
            sleepNanos = noWorkYieldNanos > 0L ? noWorkYieldNanos : schedulingNanos;
        } else {
            sleepNanos = schedulingNanos;
        }

        waitForDelay(sleepNanos, generation);
    }

    private boolean acquirePermitWithPolling(final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        final long waitStartNanos = System.nanoTime();
        while (isActive(lifecycleState, generation)) {
            try {
                if (globalSemaphore.tryAcquire(PERMIT_POLL_INTERVAL_NANOS, TimeUnit.NANOSECONDS)) {
                    if (isActive(lifecycleState, generation)) {
                        globalPermitWaitNanos.add(System.nanoTime() - waitStartNanos);
                        return true;
                    }

                    globalSemaphore.release();
                    return false;
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    private void waitForDelay(final long delayNanos, final SchedulingGeneration generation) {
        if (delayNanos <= Triggerable.MINIMUM_SCHEDULING_NANOS) {
            return;
        }

        try {
            generation.awaitStop(delayNanos, TimeUnit.NANOSECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String buildThreadName(final Connectable connectable, final int taskIndex) {
        return connectable.getName() + "[type=" + connectable.getComponentType() + ", id=" + connectable.getIdentifier()
                + ", group=" + connectable.getProcessGroup().getName() + "] task " + taskIndex;
    }

    private void submitTask(final String threadName, final SchedulingGeneration generation, final Runnable task) {
        final Runnable trackedTask = () -> {
            final Thread currentThread = Thread.currentThread();
            currentThread.setName(threadName);
            generation.addThread(currentThread);
            runningThreadCount.incrementAndGet();

            try {
                if (!shutdown.get() && !generation.isStopped()) {
                    task.run();
                }
            } finally {
                runningThreadCount.decrementAndGet();
                generation.removeThread(currentThread);
            }
        };

        executorService.execute(trackedTask);
    }

    private static OffsetDateTime getNextCronSchedule(final OffsetDateTime currentSchedule, final CronExpression cronExpression) {
        final OffsetDateTime now = OffsetDateTime.now();
        return cronExpression.next(now.isAfter(currentSchedule) ? now : currentSchedule);
    }

    private static class AutoWorker {
        private final long identifier;
        private final AtomicBoolean retired = new AtomicBoolean();

        private AutoWorker(final long identifier) {
            this.identifier = identifier;
        }
    }

    private record AutoWaiter(Thread thread, AutoWorker worker) {
    }

    private class AutoSchedulingState {
        private final Connectable connectable;
        private final ConnectableTask connectableTask;
        private final LifecycleState lifecycleState;
        private final SchedulingGeneration generation;
        private final int contextCeiling;
        private final boolean batchingSupported;
        private final boolean source;
        private final StandardAutoSchedulingController controller;
        private final AutoSchedulingMetrics metrics = new AutoSchedulingMetrics();
        private final AtomicReference<SchedulingSettings> policy;
        private final AtomicInteger activeInvocations = new AtomicInteger();
        private final ConcurrentMap<Long, AutoWorker> workers = new ConcurrentHashMap<>();
        private final AtomicLong workerSequence = new AtomicLong();
        private final AtomicBoolean sourceProbe = new AtomicBoolean();
        private final AtomicLong resetSequence = new AtomicLong();
        private final long evaluationIntervalNanos;
        private final long baseSourceDelayNanos;
        private final long maximumSourceDelayNanos;

        private volatile long nextEvaluationNanos;
        private volatile long lastEvaluationNanos = System.nanoTime();
        private volatile long lastControlSampleNanos = System.nanoTime();
        private volatile long previousBacklog;
        private volatile long sourceDelayNanos;
        private volatile long nextSourceProbeNanos;
        private volatile long lastProductiveNanos;
        private volatile AutoSchedulingDecision lastDecision;
        private volatile AutoSchedulingDecision lastChangeDecision;
        private volatile AutoSchedulingObservation lastObservation;
        private volatile boolean previousPrimaryNode;
        private volatile boolean measurementsSupported;

        private AutoSchedulingState(final Connectable connectable, final ConnectableTask connectableTask, final LifecycleState lifecycleState,
                                    final SchedulingGeneration generation, final int contextCeiling, final boolean batchingSupported, final boolean serial) {
            this.connectable = connectable;
            this.connectableTask = connectableTask;
            this.lifecycleState = lifecycleState;
            this.generation = generation;
            this.contextCeiling = contextCeiling;
            this.batchingSupported = batchingSupported;
            this.policy = new AtomicReference<>(new SchedulingSettings(1, batchingSupported ? TimeUnit.MILLISECONDS.toNanos(25L) : 0L));
            this.source = connectable.isTriggerWhenEmpty() || !connectable.hasIncomingConnection() || !Connectables.hasNonLoopConnection(connectable);
            this.controller = new StandardAutoSchedulingController(contextCeiling, batchingSupported, serial);
            final double stagger = ((Math.floorMod(connectable.getIdentifier().hashCode(), 201) - 100) / 1000D);
            this.evaluationIntervalNanos = (long) (TimeUnit.SECONDS.toNanos(1L) * (1D + stagger));
            this.nextEvaluationNanos = lastEvaluationNanos + evaluationIntervalNanos;
            this.baseSourceDelayNanos = Math.max(noWorkYieldNanos, TimeUnit.MILLISECONDS.toNanos(1L));
            this.maximumSourceDelayNanos = Math.max(baseSourceDelayNanos, TimeUnit.MILLISECONDS.toNanos(100L));
            this.previousPrimaryNode = flowController.isPrimary();
        }

        synchronized List<AutoWorker> resizeWorkers(final int targetWorkerCount) {
            final List<AutoWorker> activeWorkers = new ArrayList<>();
            for (final AutoWorker worker : workers.values()) {
                if (!worker.retired.get()) {
                    activeWorkers.add(worker);
                }
            }

            if (activeWorkers.size() > targetWorkerCount) {
                activeWorkers.sort(Comparator.comparingLong(worker -> worker.identifier));
                for (int index = targetWorkerCount; index < activeWorkers.size(); index++) {
                    activeWorkers.get(index).retired.set(true);
                }

                generation.signalChange();
                generation.signalAdmissionChange();
                return List.of();
            }

            final List<AutoWorker> newWorkers = new ArrayList<>();
            for (int index = activeWorkers.size(); index < targetWorkerCount; index++) {
                final AutoWorker worker = new AutoWorker(workerSequence.getAndIncrement());
                workers.put(worker.identifier, worker);
                newWorkers.add(worker);
            }

            return newWorkers;
        }

        void workerStopped(final AutoWorker worker) {
            workers.remove(worker.identifier, worker);
            generation.signalChange();
            generation.signalAdmissionChange();
        }

        boolean tryAdmitWorker(final SchedulingSettings expectedPolicy) {
            while (true) {
                if (policy.get() != expectedPolicy) {
                    return false;
                }

                final int current = activeInvocations.get();
                if (current >= expectedPolicy.concurrentTasks()) {
                    return false;
                }

                if (activeInvocations.compareAndSet(current, current + 1)) {
                    if (policy.get() == expectedPolicy) {
                        return true;
                    }

                    activeInvocations.decrementAndGet();
                    generation.signalAdmissionChange();
                    return false;
                }
            }
        }

        void releaseWorkerAdmission() {
            activeInvocations.decrementAndGet();
            generation.signalAdmissionChange();
        }

        boolean isSourceProbeEligible() {
            return !source || connectableTask.hasLocallyConsumableInput() || sourceDelayNanos == 0L
                    || (System.nanoTime() >= nextSourceProbeNanos && !sourceProbe.get());
        }

        boolean acquireSourceProbe() {
            return requiresSourceProbe() && sourceProbe.compareAndSet(false, true);
        }

        boolean requiresSourceProbe() {
            return source && !connectableTask.hasLocallyConsumableInput() && sourceDelayNanos > 0L;
        }

        void releaseSourceProbe() {
            sourceProbe.set(false);
            generation.signalChange();
        }

        long getSourceProbeDelayNanos() {
            return Math.max(0L, nextSourceProbeNanos - System.nanoTime());
        }

        void recordInvocationResult(final InvocationResult result) {
            if (!source) {
                return;
            }

            if (result.getOutcome() == InvocationOutcome.INVOKED_WITH_ACTIVITY) {
                sourceDelayNanos = 0L;
                nextSourceProbeNanos = 0L;
                lastProductiveNanos = System.nanoTime();
                generation.signalChange();
            } else if (result.getOutcome() == InvocationOutcome.INVOKED_WITHOUT_ACTIVITY
                    && activeInvocations.get() <= 1
                    && !connectableTask.hasLocallyConsumableInput()) {
                sourceDelayNanos = sourceDelayNanos == 0L ? baseSourceDelayNanos : Math.min(maximumSourceDelayNanos, sourceDelayNanos * 2L);
                nextSourceProbeNanos = System.nanoTime() + sourceDelayNanos;
            }
        }

        long getNextQueueDeadlineMillis() {
            long earliestDeadline = 0L;
            for (final FlowFileQueue queue : generation.queues) {
                final long deadline = queue.getNextFlowFileAvailabilityTimeMillis();
                if (deadline > 0L && (earliestDeadline == 0L || deadline < earliestDeadline)) {
                    earliestDeadline = deadline;
                }
            }

            return earliestDeadline;
        }

        boolean isEvaluationDue(final long nowNanos) {
            if (nowNanos < nextEvaluationNanos) {
                return false;
            }

            nextEvaluationNanos = nowNanos + evaluationIntervalNanos;
            return true;
        }

        void recordControlSample(final long nowNanos) {
            final SchedulingSettings currentPolicy = policy.get();
            final long sampleDurationNanos = Math.max(1L, nowNanos - lastControlSampleNanos);
            lastControlSampleNanos = nowNanos;
            metrics.recordControlSample(activeInvocations.get(), currentPolicy.concurrentTasks(),
                    connectableTask.getReadinessOutcome(), sampleDurationNanos);
        }

        AutoSchedulingObservation snapshotObservation(final long nowNanos) {
            final SchedulingSettings currentPolicy = policy.get();
            long backlog = 0L;
            for (final Connection connection : connectable.getIncomingConnections()) {
                backlog += connection.getFlowFileQueue().getLocalQueueSize().getObjectCount();
            }

            final boolean inputBacklogged = connectableTask.hasLocallyConsumableInput();
            final double backlogTrend = backlog - previousBacklog;
            previousBacklog = backlog;
            final long durationNanos = Math.max(1L, nowNanos - lastEvaluationNanos);
            lastEvaluationNanos = nowNanos;
            final double occupancy = Math.min(1D, activeInvocations.get() / (double) currentPolicy.concurrentTasks());
            final boolean currentPrimaryNode = flowController.isPrimary();
            final boolean primaryNodeChanged = currentPrimaryNode != previousPrimaryNode;
            previousPrimaryNode = currentPrimaryNode;
            final boolean sourceDemand = source && sourceDelayNanos == 0L
                    && (activeInvocations.get() > 0 || lastProductiveNanos > 0L && nowNanos - lastProductiveNanos < TimeUnit.SECONDS.toNanos(2));
            final AutoSchedulingObservation observation = metrics.snapshot(nowNanos, durationNanos, currentPolicy, occupancy,
                    connectableTask.isReady(), inputBacklogged, backlogTrend, sourceDemand, primaryNodeChanged);
            if (observation.committedFlowFiles() > 0L) {
                measurementsSupported = true;
            }

            lastObservation = observation;
            return observation;
        }

        void resetMeasurementsAfterEvent() {
            final long nowNanos = System.nanoTime();
            final SchedulingSettings currentPolicy = policy.get();
            // Workers compare settings by identity so a reset also ends batches whose values have not changed.
            policy.set(new SchedulingSettings(currentPolicy.concurrentTasks(), currentPolicy.runDurationNanos()));
            lastEvaluationNanos = nowNanos;
            lastControlSampleNanos = nowNanos;
            long backlog = 0L;
            for (final Connection connection : connectable.getIncomingConnections()) {
                backlog += connection.getFlowFileQueue().getLocalQueueSize().getObjectCount();
            }

            previousBacklog = backlog;
            lastObservation = null;
            generation.signalChange();
        }

        void applySettings(final SchedulingSettings settings) {
            final SchedulingSettings currentPolicy = policy.get();
            if (!currentPolicy.equals(settings)) {
                policy.set(new SchedulingSettings(settings.concurrentTasks(), settings.runDurationNanos()));
                generation.signalChange();
                generation.signalAdmissionChange();
                logger.info("Automatic scheduling settings changed for {} from {} to {}", connectable, currentPolicy, settings);
            }
        }

        void recordDecision(final AutoSchedulingDecision decision) {
            lastDecision = decision;
            if (decision.reason() == AutoDecisionReason.CANDIDATE_ACCEPTED || decision.reason() == AutoDecisionReason.CANDIDATE_REJECTED
                    || decision.reason() == AutoDecisionReason.RESET) {
                lastChangeDecision = decision;
            }
        }

        AutoSchedulingDiagnostics getDiagnostics() {
            final SchedulingSettings currentPolicy = policy.get();
            final AutoSchedulingDecision currentDecision = lastDecision;
            final AutoSchedulingDecision currentChangeDecision = lastChangeDecision;
            final AutoSchedulingObservation currentObservation = lastObservation;
            final long work = currentObservation == null ? 0L : currentObservation.committedFlowFiles();
            final long durationNanos = currentObservation == null ? 0L : currentObservation.durationNanos();
            final double throughput = durationNanos == 0L ? 0D : work / (durationNanos / (double) TimeUnit.SECONDS.toNanos(1L));
            final String phase = currentDecision == null ? AutoControllerPhase.HOLD.name() : currentDecision.phase().name();
            final AutoDecisionReason reason = currentDecision == null ? AutoDecisionReason.INSUFFICIENT_EVIDENCE : currentDecision.reason();
            final String lastChangeReason = currentChangeDecision == null ? null : currentChangeDecision.reason().name();
            return new AutoSchedulingDiagnostics("adaptive", contextCeiling, currentPolicy.concurrentTasks(), activeInvocations.get(),
                    TimeUnit.NANOSECONDS.toMillis(currentPolicy.runDurationNanos()), phase, reason.name(), throughput, TimeUnit.NANOSECONDS.toMillis(durationNanos),
                    lastChangeReason, reason == AutoDecisionReason.INSUFFICIENT_EVIDENCE, measurementsSupported);
        }
    }

    private class SchedulingGeneration {
        private final CountDownLatch stopSignal = new CountDownLatch(1);
        private final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        private final Set<AutoWaiter> waiters = ConcurrentHashMap.newKeySet();
        private final Set<AutoWaiter> admissionWaiters = ConcurrentHashMap.newKeySet();
        private final Set<FlowFileQueue> queues = ConcurrentHashMap.newKeySet();
        private final List<QueueSchedulingRegistration> queueRegistrations = new ArrayList<>();
        private final AtomicBoolean interruptRequested = new AtomicBoolean();
        private final AtomicLong changeSequence = new AtomicLong();
        private final AtomicLong admissionChangeSequence = new AtomicLong();
        private volatile AutoSchedulingState autoSchedulingState;

        void setAutoSchedulingState(final AutoSchedulingState autoSchedulingState) {
            this.autoSchedulingState = autoSchedulingState;
        }

        AutoSchedulingState getAutoSchedulingState() {
            return autoSchedulingState;
        }

        void addQueue(final FlowFileQueue queue) {
            queues.add(queue);
        }

        synchronized void addQueueRegistration(final QueueSchedulingRegistration registration) {
            if (isStopped()) {
                registration.close();
            } else {
                queueRegistrations.add(registration);
            }
        }

        synchronized void clearQueueRegistrations() {
            for (final QueueSchedulingRegistration registration : queueRegistrations) {
                registration.close();
            }

            queueRegistrations.clear();
            queues.clear();
        }

        long getChangeSequence() {
            return changeSequence.get();
        }

        int getWaiterCount() {
            return waiters.size();
        }

        long getAdmissionChangeSequence() {
            return admissionChangeSequence.get();
        }

        int getAdmissionWaiterCount() {
            return admissionWaiters.size();
        }

        void signalChange() {
            changeSequence.incrementAndGet();
            final AutoSchedulingState state = autoSchedulingState;
            final int wakeLimit = state == null ? waiters.size() : state.policy.get().concurrentTasks();
            int awakened = 0;
            for (final AutoWaiter waiter : waiters) {
                if (waiter.worker().retired.get()) {
                    waiters.remove(waiter);
                    continue;
                }

                LockSupport.unpark(waiter.thread());
                if (++awakened >= wakeLimit) {
                    break;
                }
            }
        }

        void awaitChange(final long expectedSequence, final long delayNanos, final AutoWorker worker) {
            final Thread currentThread = Thread.currentThread();
            final AutoWaiter waiter = new AutoWaiter(currentThread, worker);
            waiters.add(waiter);
            try {
                if (!isStopped() && !worker.retired.get() && changeSequence.get() == expectedSequence) {
                    LockSupport.parkNanos(Math.max(1L, delayNanos));
                }
            } finally {
                waiters.remove(waiter);
            }
        }

        void signalAdmissionChange() {
            admissionChangeSequence.incrementAndGet();
            for (final AutoWaiter waiter : admissionWaiters) {
                if (waiter.worker().retired.get()) {
                    admissionWaiters.remove(waiter);
                    continue;
                }

                LockSupport.unpark(waiter.thread());
            }
        }

        void awaitAdmissionChange(final long expectedSequence, final AutoWorker worker) {
            final Thread currentThread = Thread.currentThread();
            final AutoWaiter waiter = new AutoWaiter(currentThread, worker);
            admissionWaiters.add(waiter);
            try {
                while (!isStopped() && !worker.retired.get() && admissionChangeSequence.get() == expectedSequence) {
                    LockSupport.park();
                }
            } finally {
                admissionWaiters.remove(waiter);
            }
        }

        void addThread(final Thread thread) {
            threads.add(thread);

            if (interruptRequested.get()) {
                thread.interrupt();
            }
        }

        void removeThread(final Thread thread) {
            threads.remove(thread);
        }

        void stop(final boolean interrupt) {
            if (interrupt) {
                interruptRequested.set(true);
            }

            stopSignal.countDown();
            signalChange();
            signalAdmissionChange();
            clearQueueRegistrations();

            if (interruptRequested.get()) {
                for (final Thread thread : threads) {
                    thread.interrupt();
                }
            }
        }

        boolean isStopped() {
            return stopSignal.getCount() == 0L;
        }

        void awaitStop(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
            stopSignal.await(timeout, timeUnit);
        }
    }
}
