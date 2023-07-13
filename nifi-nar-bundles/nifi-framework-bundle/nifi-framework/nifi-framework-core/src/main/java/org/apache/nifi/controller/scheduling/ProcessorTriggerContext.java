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
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.util.Connectables;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ProcessorTriggerContext {
    private static final int MAX_CONCURRENT_TASKS = 20;

    private final ProcessorNode processor;
    private final LifecycleState lifecycleState;
    private final boolean isSourceComponent;
    private final ConnectableTask connectableTask;
    private final boolean isTriggerWhenAnyDestinationAvailable;
    private final List<FlowFileQueue> downstreamQueues;
    private final List<FlowFileQueue> upstreamQueues;
    private final boolean batchSupported;
    private final int maxConcurrentTasks;
    private final AtomicInteger activeTasks = new AtomicInteger(0);

    public ProcessorTriggerContext(final ProcessorNode processor, final LifecycleState lifecycleState, final ConnectableTask connectableTask) {
        this.processor = processor;
        this.lifecycleState = lifecycleState;
        this.connectableTask = connectableTask;

        boolean hasNonLoopConnection = Connectables.hasNonLoopConnection(processor);
        this.isSourceComponent = processor.isTriggerWhenEmpty()
            || !processor.hasIncomingConnection()   // No input connections
            || !hasNonLoopConnection;  // Every incoming connection loops back to itself, no inputs from other components

        isTriggerWhenAnyDestinationAvailable = processor.isTriggerWhenAnyDestinationAvailable();
        downstreamQueues = new ArrayList<>();
        for (final Connection connection : processor.getConnections()) {
            final FlowFileQueue queue = connection.getFlowFileQueue();
            downstreamQueues.add(queue);
        }

        upstreamQueues = processor.getIncomingConnections().stream()
            .map(Connection::getFlowFileQueue)
            .collect(Collectors.toList());
        batchSupported = processor.isSessionBatchingSupported();

        maxConcurrentTasks = processor.isTriggeredSerially() ? 1 : 6;
    }

    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
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
        final int taskCount = activeTasks.incrementAndGet();
        try {
            if (taskCount < maxConcurrentTasks) {
                return connectableTask.invoke();
            } else {
                return InvocationResult.DO_NOT_YIELD;
            }
        } finally {
            activeTasks.decrementAndGet();
        }
    }

    public InvocationResult invoke(final long runDurationNanos, final long maxIterations) {
        final int taskCount = activeTasks.incrementAndGet();
        try {
            if (taskCount < maxConcurrentTasks) {
                return connectableTask.invoke(runDurationNanos, maxIterations);
            } else {
                return InvocationResult.DO_NOT_YIELD;
            }
        } finally {
            activeTasks.decrementAndGet();
        }
    }

    public boolean isSourceComponent() {
        return isSourceComponent;
    }

    public boolean isWorkToDo() {
        // If it is not a 'source' component, it requires a FlowFile to process.
        final boolean dataAvailable = isSourceComponent || Connectables.flowFilesQueued(processor);
        if (!dataAvailable) {
            return false;
        }

        final boolean backpressure = isBackPressureApplied();
        return !backpressure;
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
            for (final FlowFileQueue queue : downstreamQueues) {
                if (queue.isFull()) {
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
