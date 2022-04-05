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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.engine.FlowEngine;

import java.util.ArrayList;
import java.util.List;

public class AutomaticSchedulingAgent extends AbstractTimeBasedSchedulingAgent {
    private final ProcessorTaskQueue processorTaskQueue = new ProcessorTaskQueue(this::createConnectableTask);

    private final List<AutoScheduledProcessorTrigger> autoScheduledProcessorTriggers = new ArrayList<>();

    public AutomaticSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final RepositoryContextFactory contextFactory,
                                      final PropertyEncryptor encryptor) {
        super(flowEngine, flowController, contextFactory, encryptor);

        for (int i=0; i < flowEngine.getCorePoolSize(); i++) {
            final AutoScheduledProcessorTrigger trigger = new AutoScheduledProcessorTrigger(processorTaskQueue);
            autoScheduledProcessorTriggers.add(trigger);
            flowEngine.submit(trigger);
        }
    }

    private ConnectableTask createConnectableTask(final ProcessorNode processor, final LifecycleState lifecycleState) {
        return new ConnectableTask(this, processor, flowController, contextFactory, lifecycleState, encryptor);
    }


    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public void setMaxThreadCount(final int maxThreads) {
    }

    @Override
    public void shutdown() {
        flowEngine.shutdown();
        autoScheduledProcessorTriggers.forEach(AutoScheduledProcessorTrigger::stop);
    }

    @Override
    protected void doSchedule(final Connectable connectable, final LifecycleState scheduleState) {
        if (!(connectable instanceof ProcessorNode)) {
            throw new IllegalArgumentException("Automatic Scheduling Agent is not applicable for " + connectable);
        }

        processorTaskQueue.addProcessor((ProcessorNode) connectable, scheduleState);
        logger.info("Scheduled {} to run with automatic scheduling", connectable);
    }

    @Override
    protected void doUnschedule(final Connectable connectable, final LifecycleState scheduleState) {
        if (connectable instanceof ProcessorNode) {
            processorTaskQueue.removeProcessor((ProcessorNode) connectable);
        }

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    protected void doSchedule(final ReportingTaskNode connectable, final LifecycleState scheduleState) {
        throw new UnsupportedOperationException("Automatic Scheduling Agent is not applicable for Reporting Tasks");
    }

    @Override
    protected void doUnschedule(final ReportingTaskNode connectable, final LifecycleState scheduleState) {
        throw new UnsupportedOperationException("Automatic Scheduling Agent is not applicable for Reporting Tasks");
    }
}
