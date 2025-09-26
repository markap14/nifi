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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class StandaloneProcessGroupLifecycle implements ProcessGroupLifecycle {
    private final ProcessGroup processGroup;
    private final ControllerServiceProvider controllerServiceProvider;

    public StandaloneProcessGroupLifecycle(final ProcessGroup processGroup, final ControllerServiceProvider controllerServiceProvider) {
        this.processGroup = processGroup;
        this.controllerServiceProvider = controllerServiceProvider;
    }

    @Override
    public CompletableFuture<Void> enableControllerServices() {
        final Set<ControllerServiceNode> controllerServices = processGroup.findAllControllerServices();
        if (controllerServices.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return controllerServiceProvider.enableControllerServicesAsync(controllerServices);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices() {
        final Set<ControllerServiceNode> controllerServices = processGroup.findAllControllerServices();
        if (controllerServices.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return controllerServiceProvider.disableControllerServicesAsync(controllerServices);
    }

    @Override
    public CompletableFuture<Void> startProcessors() {
        final Collection<ProcessorNode> processors = processGroup.getProcessors();
        final List<CompletableFuture<Void>> startFutures = new ArrayList<>();
        for (final ProcessorNode processor : processors) {
            startFutures.add(processGroup.startProcessor(processor, true));
        }

        return CompletableFuture.allOf(startFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> stopProcessors() {
        final Collection<ProcessorNode> processors = processGroup.getProcessors();
        final List<CompletableFuture<Void>> stopFutures = new ArrayList<>();
        for (final ProcessorNode processor : processors) {
            final CompletableFuture<Void> stopFuture = processGroup.stopProcessor(processor);
            stopFutures.add(stopFuture);
        }

        return CompletableFuture.allOf(stopFutures.toArray(new CompletableFuture[0]));
    }
}
