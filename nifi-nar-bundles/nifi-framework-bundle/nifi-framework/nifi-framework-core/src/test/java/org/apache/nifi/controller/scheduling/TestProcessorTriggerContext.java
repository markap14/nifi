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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestProcessorTriggerContext {

    private ProcessorNode processorNode;

    @BeforeEach
    public void setup() {
        processorNode = Mockito.mock(ProcessorNode.class);
        when(processorNode.getIncomingConnections()).thenReturn(Collections.emptyList());
        when(processorNode.getConnections()).thenReturn(Collections.emptySet());
    }

    @Test
    public void testCalculateMaxTasksVeryExpensiveProcessor() {
        final ProcessorTriggerContext context = createContext(TimeUnit.SECONDS.toNanos(10L), TimeUnit.MILLISECONDS.toNanos(1), 0.8D);
        final int calculated = context.calculateMaxConcurrentTasks();
        assertEquals(context.getConcurrentTaskLimit(), calculated);
    }

    @Test
    public void testCalculateMaxTasksVeryQuickProcessor() {
        final ProcessorTriggerContext context = createContext(50L, TimeUnit.MILLISECONDS.toNanos(1), 0.2D);
        final int calculated = context.calculateMaxConcurrentTasks();
        assertEquals(1, calculated);
    }

    @Test
    public void testCalculateMaxTasksVeryExpensiveButFewFlowFiles() {
        final ProcessorTriggerContext context = createContext(TimeUnit.SECONDS.toNanos(1L), TimeUnit.MILLISECONDS.toNanos(1), (1D/10000));
        final int calculated = context.calculateMaxConcurrentTasks();

        // We don't want to make an assertion about the exact value here. It should be low, though.
        assertTrue(calculated >= 1 && calculated <= 4);
    }

    @Test
    public void testCalculateMaxTasksAverageProcessorAverageQueue() {
        final ProcessorTriggerContext context = createContext(TimeUnit.MILLISECONDS.toNanos(1L), TimeUnit.MILLISECONDS.toNanos(1), 0.5D);
        final int calculated = context.calculateMaxConcurrentTasks();

        // We don't want to make an assertion about the exact value here. It should be low, though.
        assertTrue(calculated >= 1 && calculated <= 4);
    }

    @Test
    public void testCalculateMaxTasksVeryQuickProcessorFullQueue() {
        final ProcessorTriggerContext context = createContext(50L, TimeUnit.MILLISECONDS.toNanos(1), 1D);
        final int calculated = context.calculateMaxConcurrentTasks();

        // We don't want to make an assertion about the exact value here. It should be low, though.
        assertTrue(calculated >= 2);
    }

    private ProcessorTriggerContext createContext(final long nanosPerInvocation, final long aggregateNanosPerInvocation, final double queueFullRatio) {
        return new ProcessorTriggerContext(processorNode, null, null) {
            @Override
            protected long getNanosPerInvocation() {
                return nanosPerInvocation;
            }

            @Override
            protected long getAggregateNanosPerInvocation() {
                return aggregateNanosPerInvocation;
            }

            @Override
            public double getIncomingQueueFullRatio() {
                return queueFullRatio;
            }
        };
    }
}
