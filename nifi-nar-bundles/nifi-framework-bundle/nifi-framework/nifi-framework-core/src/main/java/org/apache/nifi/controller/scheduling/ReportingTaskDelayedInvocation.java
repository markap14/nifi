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

import org.apache.nifi.controller.tasks.DelayedInvocation;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;

public class ReportingTaskDelayedInvocation implements DelayedInvocation {
    private final ReportingTaskWrapper reportingTaskWrapper;
    private final ScheduleCalculator scheduleCalculator;

    public ReportingTaskDelayedInvocation(final ReportingTaskWrapper reportingTaskWrapper, final ScheduleCalculator scheduleCalculator) {
        this.reportingTaskWrapper = reportingTaskWrapper;
        this.scheduleCalculator = scheduleCalculator;
    }


    public long invoke() {
        // Call the task. It will return a boolean indicating whether or not we should yield
        // based on a lack of work for to do for the component.
        reportingTaskWrapper.run();
        return scheduleCalculator.calculateNextTriggerTime();
    }
}
