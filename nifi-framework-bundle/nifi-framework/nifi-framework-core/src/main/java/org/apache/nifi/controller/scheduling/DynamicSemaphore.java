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

import java.util.concurrent.Semaphore;

/**
 * A semaphore wrapper that supports dynamically adjusting the number of permits.
 * Uses fair ordering to prevent starvation of any particular processor's virtual threads.
 */
public class DynamicSemaphore {
    private final ResizableSemaphore semaphore;
    private volatile int maxPermits;

    public DynamicSemaphore(final int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("Permits must be at least 1");
        }
        this.maxPermits = permits;
        this.semaphore = new ResizableSemaphore(permits);
    }

    public void acquire() throws InterruptedException {
        semaphore.acquire();
    }

    public void release() {
        semaphore.release();
    }

    /**
     * Adjusts the number of available permits to the specified count. If the new count
     * is greater than the current maximum, additional permits are released. If the new count
     * is less than the current maximum, permits are reduced (threads currently holding
     * permits are not interrupted; the reduction takes effect as permits are returned).
     *
     * @param newMaxPermits the desired number of permits (must be at least 1)
     */
    public synchronized void setMaxPermits(final int newMaxPermits) {
        if (newMaxPermits < 1) {
            throw new IllegalArgumentException("Max permits must be at least 1");
        }
        final int delta = newMaxPermits - this.maxPermits;
        this.maxPermits = newMaxPermits;
        if (delta > 0) {
            semaphore.release(delta);
        } else if (delta < 0) {
            semaphore.reducePermits(-delta);
        }
    }

    public int getMaxPermits() {
        return maxPermits;
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }

    /**
     * Extends Semaphore to expose the protected {@link #reducePermits(int)} method,
     * which is needed for dynamically shrinking the pool of available permits.
     */
    private static class ResizableSemaphore extends Semaphore {
        ResizableSemaphore(final int permits) {
            super(permits, true);
        }

        @Override
        protected void reducePermits(final int reduction) {
            super.reducePermits(reduction);
        }
    }
}
