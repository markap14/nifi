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

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.InvocationStats;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ProcessorTriggerContext {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorTriggerContext.class);

    private static final long MAX_RUN_DURATION_NANOS = TimeUnit.MILLISECONDS.toNanos(50L);
    public static final int CONCURRENT_TASK_LIMIT = 12;
    public static final int DEFAULT_MAX_CONCURRENT_TASKS = 2;
    private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

    private final ProcessorNode processor;
    private final LifecycleState lifecycleState;
    private final boolean isSourceComponent;
    private final ConnectableTask connectableTask;
    private final boolean isTriggerWhenAnyDestinationAvailable;
    private final FlowFileQueue[] downstreamQueues;
    private final FlowFileQueue[] upstreamQueues;
    private final boolean batchSupported;
    private final AtomicInteger maxConcurrentTasks;
    private final AtomicLong runDurationNanos;
    private final AtomicInteger activeTasks = new AtomicInteger(0);

    private final AtomicLong selfInvocationCount = new AtomicLong(0L);
    private final AtomicLong selfInvocationNanos = new AtomicLong(0L);
    private static final AtomicLong allComponentsInvocationCount = new AtomicLong(0L);
    private static final AtomicLong allComponentsInvocationNanos = new AtomicLong(0L);


    public ProcessorTriggerContext(final ProcessorNode processor, final LifecycleState lifecycleState, final ConnectableTask connectableTask) {
        this.processor = processor;
        this.lifecycleState = lifecycleState;
        this.connectableTask = connectableTask;

        boolean hasNonLoopConnection = Connectables.hasNonLoopConnection(processor);
        this.isSourceComponent = processor.isTriggerWhenEmpty()
            || !processor.hasIncomingConnection()   // No input connections
            || !hasNonLoopConnection;  // Every incoming connection loops back to itself, no inputs from other components

        isTriggerWhenAnyDestinationAvailable = processor.isTriggerWhenAnyDestinationAvailable();
        final List<FlowFileQueue> downstreamQueueList = new ArrayList<>();
        for (final Connection connection : processor.getConnections()) {
            final FlowFileQueue queue = connection.getFlowFileQueue();
            downstreamQueueList.add(queue);
        }
        this.downstreamQueues = downstreamQueueList.toArray(new FlowFileQueue[0]);

        final List<FlowFileQueue> upstreamQueueList = processor.getIncomingConnections().stream()
            .map(Connection::getFlowFileQueue)
            .collect(Collectors.toList());
        this.upstreamQueues = upstreamQueueList.toArray(new FlowFileQueue[0]);

        batchSupported = processor.isSessionBatchingSupported();

        maxConcurrentTasks = new AtomicInteger(processor.isTriggeredSerially() ? 1 : DEFAULT_MAX_CONCURRENT_TASKS);

        final long configuredRunDuration = processor.getRunDuration(TimeUnit.NANOSECONDS);
        runDurationNanos = new AtomicLong(processor.isSessionBatchingSupported() ? configuredRunDuration : 0);
    }

    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks.get();
    }

    public boolean isBatchSupported() {
        return batchSupported;
    }

    public ProcessorNode getProcessor() {
        return processor;
    }

    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public InvocationResult invoke() {
        return invoke(connectableTask::invoke);
    }

    // TODO: Now that Run Duration is calculated in this class, it's weird to pass it in. Do we even need Max Iterations at this point either?
    public InvocationResult invoke(final long runDurationNanos, final long maxIterations) {
        return invoke(() -> connectableTask.invoke(runDurationNanos, maxIterations));
    }

    private InvocationResult invoke(final Supplier<InvocationResult> result) {
        final int taskCount = activeTasks.incrementAndGet();
        try {
            if (taskCount >= maxConcurrentTasks.get()) {
                return InvocationResult.NO_INVOCATIONS;
            }

            final InvocationResult invocationResult = result.get();
            updateInvocationStats(invocationResult.getStats());

            return invocationResult;
        } finally {
            activeTasks.decrementAndGet();
        }
    }

    private void updateInvocationStats(final InvocationStats invocationStats) {
        selfInvocationCount.addAndGet(invocationStats.getInvocations());
        selfInvocationNanos.addAndGet(invocationStats.getNanos());
        allComponentsInvocationCount.addAndGet(invocationStats.getInvocations());
        allComponentsInvocationNanos.addAndGet(invocationStats.getNanos());
    }

    public void recalculateSchedulingParameters() {
        final int newValue = calculateMaxConcurrentTasks();
        final int previousValue = maxConcurrentTasks.getAndSet(newValue);

        if (newValue == previousValue) {
            logger.debug("Max Concurrent Tasks for {} will remain at {}", processor, previousValue);
        } else {
            logger.debug("Max Concurrent Tasks for {} changed from {} to {}", processor, previousValue, newValue);
        }


        final long newRunDurationNanos = calculateRunDurationNanos();
        final long previousRunDurationNanos = runDurationNanos.getAndSet(newRunDurationNanos);

        if (newRunDurationNanos == previousRunDurationNanos) {
            logger.debug("Run Duration for {} will remain at {}", processor, previousRunDurationNanos);
        } else {
            logger.debug("Run Duration for {} changed from {} to {}", processor, previousRunDurationNanos, newRunDurationNanos);
        }
    }

    public long getRunDurationNanos() {
        return runDurationNanos.get();
    }

    int calculateMaxConcurrentTasks() {
        if (processor.isTriggeredSerially()) {
            return 1;
        }

        final long nanosPerInvocation = getNanosPerInvocation();
        if (nanosPerInvocation == 0L) {
            return DEFAULT_MAX_CONCURRENT_TASKS;
        }

        final double aggregateNanosPerInvocation = getAggregateNanosPerInvocation();

        final double invocationRatio = (double) nanosPerInvocation / aggregateNanosPerInvocation;
        final double queueFullRatio = getIncomingQueueFullRatio();
        int maxThreads = 1 + (int) (invocationRatio * queueFullRatio * 2);

        // If queue is at least half full, allow no fewer than 2 tasks.
        if (queueFullRatio >= 0.5D) {
            maxThreads = Math.max(maxThreads, 2);
        }

        return Math.min(maxThreads, getConcurrentTaskLimit());
    }

    protected int getConcurrentTaskLimit() {
        return Math.min(CONCURRENT_TASK_LIMIT, NUM_CORES);
    }

    protected long getNanosPerInvocation() {
        final long selfNanos = selfInvocationNanos.get();
        final long selfCount = selfInvocationCount.get();
        if (selfCount == 0L) {
            return 0;
        }

        final long nanosPerInvocation = selfNanos / selfCount;
        return nanosPerInvocation;
    }

    protected long getAggregateNanosPerInvocation() {
        final long aggregateNanos = allComponentsInvocationNanos.get();
        final long aggregateCount = allComponentsInvocationCount.get();
        final long aggregateNanosPerInvocation = aggregateNanos / aggregateCount;
        return aggregateNanosPerInvocation;
    }

    private long calculateRunDurationNanos() {
        final double incomingFullRatio = getIncomingQueueFullRatio();
        final double outgoingFullRatio = getOutgoingQueueFullRatio();
        final double scaler = (incomingFullRatio + (1 - outgoingFullRatio)) / 2.0D;

        return Math.max(1, (long) (scaler * MAX_RUN_DURATION_NANOS));
    }


    public boolean isSourceComponent() {
        return isSourceComponent;
    }

    public boolean isWorkToDo() {
        // If it is not a 'source' component, it requires a FlowFile to process.
        final boolean dataAvailable = isSourceComponent || isFlowFileAvailable();
        if (!dataAvailable) {
            return false;
        }

        final boolean backpressure = isBackPressureApplied();
        return !backpressure;
    }

    private boolean isFlowFileAvailable() {
        // This method is called A LOT. As a result, creation of an Iterator using for-each loop
        // becomes expensive, especially on GC. To avoid that, we iterate using index style.
        for (int i=0; i < upstreamQueues.length; i++) {
            if (upstreamQueues[i].getFlowFileAvailability() == FlowFileAvailability.FLOWFILE_AVAILABLE) {
                return true;
            }
        }

        return false;
    }

    public boolean isBackPressureApplied() {
        if (isTriggerWhenAnyDestinationAvailable) {
            for (final FlowFileQueue queue : downstreamQueues) {
                if (queue.isFull()) {
                    return true;
                }
            }

            return false;
        } else {
            // This method is called A LOT. As a result, creation of an Iterator using for-each loop
            // becomes expensive, especially on GC. To avoid that, we iterate using index style.
            for (int i=0; i < downstreamQueues.length; i++) {
                if (downstreamQueues[i].isFull()) {
                    return true;
                }
            }

            return false;
        }
    }

    public double getIncomingQueueFullRatio() {
        double maxRatio = 0D;

        for (final FlowFileQueue queue : upstreamQueues) {
            final QueueSize queueSize = queue.size();
            final long backpressureThreshold = queue.getBackPressureObjectThreshold();
            if (backpressureThreshold <= 0) {
                continue;
            }

            final double objectRatio = (double) queueSize.getObjectCount() / backpressureThreshold;
            maxRatio = Math.max(maxRatio, objectRatio);
        }

        return maxRatio;
    }

    public double getOutgoingQueueFullRatio() {
        double maxRatio = 0D;
        for (final FlowFileQueue queue : downstreamQueues) {
            maxRatio = Math.max(maxRatio, queue.getFullRatio());
        }

        return maxRatio;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ProcessorTriggerContext that = (ProcessorTriggerContext) o;
        return Objects.equals(processor, that.processor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processor);
    }
}
