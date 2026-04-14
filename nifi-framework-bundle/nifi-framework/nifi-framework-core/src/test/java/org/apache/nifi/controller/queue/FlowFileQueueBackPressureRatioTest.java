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
package org.apache.nifi.controller.queue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class FlowFileQueueBackPressureRatioTest {

    /**
     * Creates a spy-based FlowFileQueue that delegates to the default methods
     * while allowing us to stub the required abstract methods.
     */
    private FlowFileQueue createQueue(final long objectThreshold, final long bytesThreshold,
                                      final int objectCount, final long byteCount) {
        final FlowFileQueue queue = spy(FlowFileQueue.class);
        when(queue.getBackPressureObjectThreshold()).thenReturn(objectThreshold);
        doReturn(bytesThreshold).when(queue).getBackPressureDataSizeThresholdBytes();
        when(queue.size()).thenReturn(new QueueSize(objectCount, byteCount));
        return queue;
    }

    @Test
    void testObjectCountRatioOnly() {
        final FlowFileQueue queue = createQueue(10000, 0, 5000, 0);
        assertEquals(0.5, queue.getBackPressureRatio(), 0.001);
    }

    @Test
    void testDataSizeRatioOnly() {
        final FlowFileQueue queue = createQueue(0, 1000, 0, 900);
        assertEquals(0.9, queue.getBackPressureRatio(), 0.001);
    }

    @Test
    void testBothThresholdsReturnsHigher() {
        final FlowFileQueue queue = createQueue(10000, 1000, 5000, 900);
        assertEquals(0.9, queue.getBackPressureRatio(), 0.001);
    }

    @Test
    void testBothThresholdsDisabledReturnsZero() {
        final FlowFileQueue queue = createQueue(0, 0, 100, 500);
        assertEquals(0.0, queue.getBackPressureRatio(), 0.001);
    }

    @Test
    void testExceedingThresholdReturnsGreaterThanOne() {
        final FlowFileQueue queue = createQueue(100, 0, 150, 0);
        assertTrue(queue.getBackPressureRatio() > 1.0);
        assertEquals(1.5, queue.getBackPressureRatio(), 0.001);
    }

    @Test
    void testEmptyQueueReturnsZero() {
        final FlowFileQueue queue = createQueue(10000, 1024 * 1024, 0, 0);
        assertEquals(0.0, queue.getBackPressureRatio(), 0.001);
    }
}
