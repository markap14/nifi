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
package org.apache.nifi.controller.tasks;

import org.apache.nifi.controller.tasks.InvocationResult.YieldReason;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InvocationResultTest {

    @Test
    void testDoNotYield() {
        final InvocationResult result = InvocationResult.doNotYield();
        assertFalse(result.isYield());
        assertEquals(YieldReason.NONE, result.getYieldReason());
        assertNull(result.getYieldExplanation());
    }

    @Test
    void testTerminated() {
        final InvocationResult result = InvocationResult.terminated();
        assertFalse(result.isYield());
        assertEquals(YieldReason.TERMINATED, result.getYieldReason());
    }

    @Test
    void testYielded() {
        final InvocationResult result = InvocationResult.yielded();
        assertFalse(result.isYield());
        assertEquals(YieldReason.YIELDED, result.getYieldReason());
    }

    @Test
    void testNoWork() {
        final InvocationResult result = InvocationResult.noWork();
        assertTrue(result.isYield());
        assertEquals(YieldReason.NO_WORK, result.getYieldReason());
        assertNotNull(result.getYieldExplanation());
    }

    @Test
    void testBackpressure() {
        final InvocationResult result = InvocationResult.backpressure();
        assertTrue(result.isYield());
        assertEquals(YieldReason.BACKPRESSURE, result.getYieldReason());
        assertNotNull(result.getYieldExplanation());
    }

    @Test
    void testNotPrimaryNode() {
        final InvocationResult result = InvocationResult.notPrimaryNode();
        assertTrue(result.isYield());
        assertEquals(YieldReason.NOT_PRIMARY_NODE, result.getYieldReason());
        assertNotNull(result.getYieldExplanation());
    }
}
