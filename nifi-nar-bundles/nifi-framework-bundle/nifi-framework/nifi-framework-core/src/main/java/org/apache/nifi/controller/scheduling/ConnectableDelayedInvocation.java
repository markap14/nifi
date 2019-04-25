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
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.DelayedInvocation;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class ConnectableDelayedInvocation implements DelayedInvocation {
    private static final Logger logger = LoggerFactory.getLogger(ConnectableDelayedInvocation.class);

    private final ConnectableTask connectableTask;
    private final Connectable connectable;
    private final long boredYieldNanos;

    public ConnectableDelayedInvocation(final ConnectableTask connectableTask, final long boredYieldNanos) {
        this.connectableTask = connectableTask;
        this.connectable = connectableTask.getConnectable();
        this.boredYieldNanos = boredYieldNanos;
    }


    public long invoke() {
        // Call the task. It will return a boolean indicating whether or not we should yield
        // based on a lack of work for to do for the component.
        final InvocationResult invocationResult = connectableTask.invoke();
        if (invocationResult.isYield()) {
            logger.debug("Yielding {} due to {}", connectable, invocationResult.getYieldExplanation());
        }

        final long nextTriggerTime = getNextTriggerTime(invocationResult);
        return nextTriggerTime;
    }


    private long getNextTriggerTime(final InvocationResult invocationResult) {
        final long yieldExpiration = connectable.getYieldExpiration();
        final long now = System.nanoTime();

        final long schedulingNanos = connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        final long nextScheduledExecution = now + schedulingNanos;

        if (yieldExpiration > now) {
            return Math.max(yieldExpiration, nextScheduledExecution);
        }

        if (boredYieldNanos > 0L && invocationResult.isYield()) {
            return Math.max(nextScheduledExecution, now + boredYieldNanos);
        }

        return nextScheduledExecution;
    }
}
