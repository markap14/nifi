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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.connector.components.ControllerServiceEnablementScope;
import org.apache.nifi.components.connector.components.ProcessGroupLifecycle;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class StandaloneProcessGroupLifecycle implements ProcessGroupLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneProcessGroupLifecycle.class);

    private final ProcessGroup processGroup;
    private final ControllerServiceProvider controllerServiceProvider;

    public StandaloneProcessGroupLifecycle(final ProcessGroup processGroup, final ControllerServiceProvider controllerServiceProvider) {
        this.processGroup = processGroup;
        this.controllerServiceProvider = controllerServiceProvider;
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final ControllerServiceEnablementScope scope) {
        final Set<ControllerServiceNode> controllerServices = scope == ControllerServiceEnablementScope.ENABLE_ALL ? processGroup.findAllControllerServices() : findReferencedServices();
        return enableControllerServices(controllerServices);
    }

    private Set<ControllerServiceNode> findReferencedServices() {
        final Set<ControllerServiceNode> referencedServices = new HashSet<>();
        collectReferencedServices(processGroup, referencedServices);
        return referencedServices;
    }

    private void collectReferencedServices(final ProcessGroup group, final Set<ControllerServiceNode> referencedServices) {
        for (final ProcessorNode processor : group.getProcessors()) {
            for (final PropertyDescriptor descriptor : processor.getPropertyDescriptors()) {
                if (descriptor.getControllerServiceDefinition() == null) {
                    continue;
                }

                final String serviceId = processor.getProperty(descriptor).getEffectiveValue(group.getParameterContext());
                if (serviceId == null) {
                    continue;
                }

                final ControllerServiceNode serviceNode = controllerServiceProvider.getControllerServiceNode(serviceId);
                if (serviceNode == null) {
                    continue;
                }

                logger.debug("Marking {} as a Referenced Controller Service because it is referenced by {} property of {}",
                    serviceNode, descriptor.getName(), processor);
                referencedServices.add(serviceNode);
            }
        }

        while (true) {
            final Set<ControllerServiceNode> newlyAddedServices = new HashSet<>();
            for (final ControllerServiceNode service : referencedServices) {
                for (final PropertyDescriptor descriptor : service.getPropertyDescriptors()) {
                    if (descriptor.getControllerServiceDefinition() == null) {
                        continue;
                    }

                    final String serviceId = service.getProperty(descriptor).getEffectiveValue(group.getParameterContext());
                    if (serviceId == null) {
                        continue;
                    }

                    final ControllerServiceNode referencedService = controllerServiceProvider.getControllerServiceNode(serviceId);
                    if (referencedService != null && !referencedServices.contains(referencedService)) {
                        logger.debug("Marking {} as a Referenced Controller Service because it is referenced by {} property of {}",
                            referencedService, descriptor.getName(), service);

                        newlyAddedServices.add(referencedService);
                    }
                }
            }

            referencedServices.addAll(newlyAddedServices);
            if (newlyAddedServices.isEmpty()) {
                break;
            }
        }

        for (final ProcessGroup childGroup : processGroup.getProcessGroups()) {
            collectReferencedServices(childGroup, referencedServices);
        }
    }

    @Override
    public CompletableFuture<Void> enableControllerServices(final Collection<String> collection) {
        final Set<ControllerServiceNode> serviceNodes = findControllerServices(collection);
        return enableControllerServices(serviceNodes);
    }

    private CompletableFuture<Void> enableControllerServices(final Set<ControllerServiceNode> serviceNodes) {
        if (serviceNodes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return controllerServiceProvider.enableControllerServicesAsync(serviceNodes);
    }

    private Set<ControllerServiceNode> findControllerServices(final Collection<String> serviceIds) {
        return processGroup.findAllControllerServices().stream()
            .filter(service -> service.getVersionedComponentId().isPresent())
            .filter(service -> serviceIds.contains(service.getVersionedComponentId().get()))
            .collect(Collectors.toSet());
    }

    @Override
    public CompletableFuture<Void> disableControllerServices() {
        final Set<ControllerServiceNode> controllerServices = processGroup.findAllControllerServices();
        return disableControllerServices(controllerServices);
    }

    private CompletableFuture<Void> disableControllerServices(final Set<ControllerServiceNode> serviceNodes) {
        if (serviceNodes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return controllerServiceProvider.disableControllerServicesAsync(serviceNodes);
    }

    @Override
    public CompletableFuture<Void> disableControllerServices(final Collection<String> collection) {
        final Set<ControllerServiceNode> serviceNodes = findControllerServices(collection);
        return disableControllerServices(serviceNodes);
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
