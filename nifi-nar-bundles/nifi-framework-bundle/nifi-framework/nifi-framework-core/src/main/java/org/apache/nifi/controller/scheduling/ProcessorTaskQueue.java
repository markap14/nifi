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
import org.apache.nifi.controller.tasks.ConnectableTask;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

public class ProcessorTaskQueue {
    private final List<ProcessorTriggerContext> triggerContexts = new CopyOnWriteArrayList<>();
    private final BiFunction<ProcessorNode, LifecycleState, ConnectableTask> connectableTaskFactory;

    public ProcessorTaskQueue(final BiFunction<ProcessorNode, LifecycleState, ConnectableTask> connectableTaskFactory) {
        this.connectableTaskFactory = connectableTaskFactory;
    }

    public void addProcessor(final ProcessorNode processor, final LifecycleState lifecycleState) {
        final ConnectableTask connectableTask = connectableTaskFactory.apply(processor, lifecycleState);
        final ProcessorTriggerContext triggerContext = new ProcessorTriggerContext(processor, lifecycleState, connectableTask);
        triggerContexts.add(triggerContext);
    }

    public void removeProcessor(final ProcessorNode processor) {
        triggerContexts.removeIf(context -> context.getProcessor() == processor);
    }

    public List<ProcessorTriggerContext> getTriggerContexts() {
        return triggerContexts;
    }

}
