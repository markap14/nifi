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

import org.quartz.CronExpression;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class CronScheduleCalculator implements ScheduleCalculator {
    private final CronExpression cronExpression;
    private final long initialTriggerTime;

    public CronScheduleCalculator(final String cronExpression) {
        try {
            this.cronExpression = new CronExpression(cronExpression);
        } catch (final Exception pe) {
            throw new IllegalArgumentException("Not a valid Cron Expression: " + cronExpression);
        }

        initialTriggerTime = calculateNextTriggerTime();
    }

    @Override
    public long getInitialTriggerTime() {
        return initialTriggerTime;
    }

    @Override
    public long calculateNextTriggerTime() {
        // Since the clock has not a millisecond precision, we have to check that we
        // schedule the next time after the time this was supposed to run, otherwise
        // we might end up with running the same task twice
        while (true) {
            final Date date = cronExpression.getTimeAfter(new Date());
            final long millis = date.getTime();
            final long delayMillis = millis - System.currentTimeMillis();

            if (delayMillis <= 0) {
                continue;
            } else {
                final long delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
                return System.nanoTime() + delayNanos;
            }
        }
    }
}
