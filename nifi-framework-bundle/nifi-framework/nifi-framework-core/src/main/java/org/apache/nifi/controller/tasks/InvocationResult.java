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

public class InvocationResult {

    public enum YieldReason {
        NONE,
        NO_WORK,
        BACKPRESSURE,
        NOT_PRIMARY_NODE,
        TERMINATED,
        YIELDED
    }

    private static final InvocationResult DO_NOT_YIELD_INSTANCE = new InvocationResult(YieldReason.NONE, null);
    private static final InvocationResult TERMINATED_INSTANCE = new InvocationResult(YieldReason.TERMINATED, null);
    private static final InvocationResult YIELDED_INSTANCE = new InvocationResult(YieldReason.YIELDED, null);
    private static final InvocationResult NO_WORK_INSTANCE = new InvocationResult(YieldReason.NO_WORK, "No work to do");
    private static final InvocationResult BACKPRESSURE_INSTANCE = new InvocationResult(YieldReason.BACKPRESSURE, "Backpressure Applied");
    private static final InvocationResult NOT_PRIMARY_NODE_INSTANCE = new InvocationResult(YieldReason.NOT_PRIMARY_NODE, "This node is not the primary node");

    private final YieldReason yieldReason;
    private final String yieldExplanation;

    private InvocationResult(final YieldReason yieldReason, final String yieldExplanation) {
        this.yieldReason = yieldReason;
        this.yieldExplanation = yieldExplanation;
    }

    /**
     * Indicates that the invocation did not perform useful work and the scheduling loop should
     * pause before invoking the connectable again. This returns true for every yield reason that
     * represents a temporary pause, including explicit yields by the component (YIELDED),
     * backpressure, no-work conditions, and non-primary-node state on clustered components.
     * TERMINATED is not treated as a yield because the scheduling loop must exit immediately on
     * termination rather than sleep and retry.
     */
    public boolean isYield() {
        return yieldReason != YieldReason.NONE && yieldReason != YieldReason.TERMINATED;
    }

    public String getYieldExplanation() {
        return yieldExplanation;
    }

    public YieldReason getYieldReason() {
        return yieldReason;
    }

    public static InvocationResult doNotYield() {
        return DO_NOT_YIELD_INSTANCE;
    }

    public static InvocationResult terminated() {
        return TERMINATED_INSTANCE;
    }

    public static InvocationResult yielded() {
        return YIELDED_INSTANCE;
    }

    public static InvocationResult noWork() {
        return NO_WORK_INSTANCE;
    }

    public static InvocationResult backpressure() {
        return BACKPRESSURE_INSTANCE;
    }

    public static InvocationResult notPrimaryNode() {
        return NOT_PRIMARY_NODE_INSTANCE;
    }
}
