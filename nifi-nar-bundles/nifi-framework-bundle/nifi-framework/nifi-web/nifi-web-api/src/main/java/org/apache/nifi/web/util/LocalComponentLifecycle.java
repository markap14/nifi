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

package org.apache.nifi.web.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.revision.RevisionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LocalComponentLifecycle implements ComponentLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(LocalComponentLifecycle.class);

    private NiFiServiceFacade serviceFacade;
    private RevisionManager revisionManager;
    private DtoFactory dtoFactory;

    @Override
    public Set<AffectedComponentEntity> scheduleComponents(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> components,
        final ScheduledState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        final Map<String, Revision> processorRevisions = components.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, entity -> revisionManager.getRevision(entity.getId())));

        final Map<String, AffectedComponentEntity> affectedComponentMap = components.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, Function.identity()));

        if (desiredState == ScheduledState.RUNNING) {
            startComponents(groupId, processorRevisions, affectedComponentMap, pause, invalidComponentAction);
        } else {
            stopComponents(groupId, processorRevisions, affectedComponentMap, pause, invalidComponentAction);
        }

        final Set<AffectedComponentEntity> updatedEntities = components.stream()
            .map(component -> AffectedComponentUtils.updateEntity(component, serviceFacade, dtoFactory))
            .collect(Collectors.toSet());
        return updatedEntities;
    }

    @Override
    public Set<AffectedComponentEntity> activateControllerServices(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> services,
        final ControllerServiceState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        final Map<String, Revision> serviceRevisions = services.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, entity -> revisionManager.getRevision(entity.getId())));

        final Map<String, AffectedComponentEntity> affectedServiceMap = services.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, Function.identity()));

        if (desiredState == ControllerServiceState.ENABLED) {
            enableControllerServices(groupId, serviceRevisions, affectedServiceMap, pause, invalidComponentAction);
        } else {
            disableControllerServices(groupId, serviceRevisions, affectedServiceMap, pause, invalidComponentAction);
        }

        return services.stream()
            .map(componentEntity -> serviceFacade.getControllerService(componentEntity.getId()))
            .map(dtoFactory::createAffectedComponentEntity)
            .collect(Collectors.toSet());
    }


    private void startComponents(final String processGroupId, final Map<String, Revision> componentRevisions, final Map<String, AffectedComponentEntity> affectedComponents, final Pause pause,
                                 final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        if (componentRevisions.isEmpty()) {
            return;
        }

        logger.debug("Starting components with ID's {} from Process Group {}", componentRevisions.keySet(), processGroupId);

        // Wait for all affected processors to be either VALID or INVALID
        waitForProcessorValidation(processGroupId, affectedComponents, pause);

        serviceFacade.verifyScheduleComponents(processGroupId, ScheduledState.RUNNING, componentRevisions.keySet());
        serviceFacade.scheduleComponents(processGroupId, ScheduledState.RUNNING, componentRevisions);

        // wait for all of the Processors to reach the desired state. We don't have to wait for other components because
        // Local and Remote Ports as well as funnels start immediately.
        waitForProcessorState(processGroupId, affectedComponents, ScheduledState.RUNNING, pause, invalidComponentAction);
    }

    private void stopComponents(final String processGroupId, final Map<String, Revision> componentRevisions, final Map<String, AffectedComponentEntity> affectedComponents, final Pause pause,
                                final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        if (componentRevisions.isEmpty()) {
            return;
        }

        logger.debug("Stopping components with ID's {} from Process Group {}", componentRevisions.keySet(), processGroupId);

        serviceFacade.verifyScheduleComponents(processGroupId, ScheduledState.STOPPED, componentRevisions.keySet());
        serviceFacade.scheduleComponents(processGroupId, ScheduledState.STOPPED, componentRevisions);

        // wait for all of the Processors to reach the desired state. We don't have to wait for other components because
        // Local and Remote Ports as well as funnels stop immediately.
        waitForProcessorState(processGroupId, affectedComponents, ScheduledState.STOPPED, pause, invalidComponentAction);
    }


    /**
     * Waits for all given Processors to complete validation
     *
     * @return <code>true</code> if all processors have completed validation, <code>false</code> if the given {@link Pause}
     *         indicated to give up before all of the processors have completed validation
     */
    private boolean waitForProcessorValidation(final String groupId, final Map<String, AffectedComponentEntity> affectedComponents, final Pause pause) {

        logger.debug("Waiting for {} processors to complete validation", affectedComponents.size());
        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ProcessorEntity> processorEntities = serviceFacade.getProcessors(groupId, true);
            if (isProcessorValidationComplete(processorEntities, affectedComponents)) {
                logger.debug("All {} processors of interest have completed validation", affectedComponents.size());
                return true;
            }

            continuePolling = pause.pause();
        }
        return false;
    }

    private boolean isProcessorValidationComplete(final Set<ProcessorEntity> processorEntities, final Map<String, AffectedComponentEntity> affectedComponents) {
        updateAffectedProcessors(processorEntities, affectedComponents);
        for (final ProcessorEntity entity : processorEntities) {
            if (!affectedComponents.containsKey(entity.getId())) {
                continue;
            }

            if (ProcessorDTO.VALIDATING.equals(entity.getComponent().getValidationStatus())) {
                logger.debug("Processor {} still has a validation state of VALIDATING", entity.getId());
                return false;
            }
        }
        return true;
    }

    /**
     * Waits for all of the given Processors to reach the given Scheduled State.
     *
     * @return <code>true</code> if all processors have reached the desired state, false if the given {@link Pause} indicates
     *         to give up before all of the processors have reached the desired state
     */
    private boolean waitForProcessorState(final String groupId, final Map<String, AffectedComponentEntity> affectedComponents,
        final ScheduledState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        logger.debug("Waiting for {} processors to transition their states to {}", affectedComponents.size(), desiredState);

        int loopCount = 0;
        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ProcessorEntity> processorEntities = serviceFacade.getProcessors(groupId, true);

            final boolean debugLog = loopCount++ % 10 == 0; // Log debug info every 10th iteration. This is typically called with a Pause of 250 ms, so don't want to log 4x per second.
            if (isProcessorActionComplete(processorEntities, affectedComponents, desiredState, invalidComponentAction, debugLog)) {
                logger.debug("All {} processors of interest now have the desired state of {}", affectedComponents.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    private void updateAffectedProcessors(final Set<ProcessorEntity> processorEntities, final Map<String, AffectedComponentEntity> affectedComponents) {
        // update the affected processors
        processorEntities.stream()
                .filter(entity -> affectedComponents.containsKey(entity.getId()))
                .forEach(entity -> {
                    final AffectedComponentEntity affectedComponentEntity = affectedComponents.get(entity.getId());
                    affectedComponentEntity.setRevision(entity.getRevision());

                    // only consider updating this component if the user has permissions to it
                    if (Boolean.TRUE.equals(affectedComponentEntity.getPermissions().getCanRead())) {
                        final AffectedComponentDTO affectedComponent = affectedComponentEntity.getComponent();
                        affectedComponent.setState(entity.getStatus().getAggregateSnapshot().getRunStatus());
                        affectedComponent.setActiveThreadCount(entity.getStatus().getAggregateSnapshot().getActiveThreadCount());

                        if (Boolean.TRUE.equals(entity.getPermissions().getCanRead())) {
                            affectedComponent.setValidationErrors(entity.getComponent().getValidationErrors());
                        }
                    }
                });
    }

    private boolean isProcessorActionComplete(final Set<ProcessorEntity> processorEntities, final Map<String, AffectedComponentEntity> affectedComponents, final ScheduledState desiredState,
                                              final InvalidComponentAction invalidComponentAction, final boolean debugLog) throws LifecycleManagementException {

        final String desiredStateName = desiredState.name();

        updateAffectedProcessors(processorEntities, affectedComponents);

        for (final ProcessorEntity entity : processorEntities) {
            if (!affectedComponents.containsKey(entity.getId())) {
                if (debugLog) {
                    logger.debug("When checking if Processor Action is complete for {}, the processor is not in the set of affected components so ignoring its state", entity.getId());
                }

                continue;
            }

            final ProcessorStatusDTO status = entity.getStatus();

            if (ProcessorDTO.INVALID.equals(entity.getComponent().getValidationStatus())) {
                if (debugLog) {
                    logger.debug("When checking if Processor Action is complete for {}, desired state = {}, validation status = {}, invalid component action = {}", entity.getId(), desiredState,
                        entity.getComponent().getValidationStatus(), invalidComponentAction);
                }

                switch (invalidComponentAction) {
                    case WAIT:
                        return false;
                    case SKIP:
                        continue;
                    case FAIL:
                        final String action = desiredState == ScheduledState.RUNNING ? "start" : "stop";
                        throw new LifecycleManagementException("Could not " + action + " " + entity.getComponent().getName() + " because it is invalid");
                }
            }

            final String runStatus = status.getAggregateSnapshot().getRunStatus();
            final boolean stateMatches = desiredStateName.equalsIgnoreCase(runStatus);
            if (!stateMatches) {
                if (debugLog && logger.isDebugEnabled()) {
                    logRunStatus(entity, status, desiredState);
                }

                return false;
            }

            if (desiredState == ScheduledState.STOPPED && status.getAggregateSnapshot().getActiveThreadCount() != 0) {
                if (debugLog) {
                    logger.debug("When checking if Processor Action is complete for {}, desired state = {} but there are {} active threads", entity.getId(), desiredState,
                        status.getAggregateSnapshot().getActiveThreadCount());
                }

                return false;
            }
        }

        return true;
    }

    private void logRunStatus(final ProcessorEntity entity, final ProcessorStatusDTO status, final ScheduledState desiredState) {
        final String runStatus = status.getAggregateSnapshot().getRunStatus();

        final List<NodeProcessorStatusSnapshotDTO> nodeSnapshots = status.getNodeSnapshots();
        if (nodeSnapshots == null) {
            logger.debug("When checking if Processor Action is complete for {}, processor run status = {}, desired state = {} so considering Processor Action NOT complete.",
                entity.getId(), runStatus, desiredState);
            return;
        }

        final Set<String> distinctRunStatuses = new HashSet<>();
        final Map<String, String> nodewiseRunStatuses = new HashMap<>();
        for (final NodeProcessorStatusSnapshotDTO nodeSnapshotDto : nodeSnapshots) {
            final String nodeRunStatus = nodeSnapshotDto.getStatusSnapshot().getRunStatus();
            final String nodeId = nodeSnapshotDto.getNodeId();
            nodewiseRunStatuses.put(nodeId, nodeRunStatus);
            distinctRunStatuses.add(nodeRunStatus);
        }

        if (distinctRunStatuses.size() == 1) {
            logger.debug("When checking if Processor Action is complete for {}, processor run status = {}, desired state = {} so considering Processor Action NOT complete. All nodes " +
                "agree on the Run Status.", entity.getId(), runStatus, desiredState);
        } else {
            logger.debug("When checking if Processor Action is complete for {}, processor run status = {}, desired state = {} so considering Processor Action NOT complete. Node-wise " +
                "breakdown of node statuses: {}", entity.getId(), runStatus, desiredState, nodewiseRunStatuses);
        }
    }

    private void enableControllerServices(final String processGroupId, final Map<String, Revision> serviceRevisions, final Map<String, AffectedComponentEntity> affectedServices, final Pause pause,
                                          final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        if (serviceRevisions.isEmpty()) {
            return;
        }

        logger.debug("Enabling Controller Services with ID's {} from Process Group {}", serviceRevisions.keySet(), processGroupId);

        waitForControllerServiceValidation(processGroupId, affectedServices, pause);

        serviceFacade.verifyActivateControllerServices(processGroupId, ControllerServiceState.ENABLED, affectedServices.keySet());
        serviceFacade.activateControllerServices(processGroupId, ControllerServiceState.ENABLED, serviceRevisions);
        waitForControllerServiceState(processGroupId, affectedServices, ControllerServiceState.ENABLED, pause, invalidComponentAction);
    }

    private void disableControllerServices(final String processGroupId, final Map<String, Revision> serviceRevisions, final Map<String, AffectedComponentEntity> affectedServices, final Pause pause,
                                           final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        if (serviceRevisions.isEmpty()) {
            return;
        }

        logger.debug("Disabling Controller Services with ID's {} from Process Group {}", serviceRevisions.keySet(), processGroupId);

        serviceFacade.verifyActivateControllerServices(processGroupId, ControllerServiceState.DISABLED, affectedServices.keySet());
        serviceFacade.activateControllerServices(processGroupId, ControllerServiceState.DISABLED, serviceRevisions);
        waitForControllerServiceState(processGroupId, affectedServices, ControllerServiceState.DISABLED, pause, invalidComponentAction);
    }

    static List<List<ControllerServiceNode>> determineEnablingOrder(final Map<String, ControllerServiceNode> serviceNodeMap) {
        final List<List<ControllerServiceNode>> orderedNodeLists = new ArrayList<>();

        for (final ControllerServiceNode node : serviceNodeMap.values()) {
            final List<ControllerServiceNode> branch = new ArrayList<>();
            determineEnablingOrder(serviceNodeMap, node, branch, new HashSet<ControllerServiceNode>());
            orderedNodeLists.add(branch);
        }

        return orderedNodeLists;
    }

    private static void determineEnablingOrder(
        final Map<String, ControllerServiceNode> serviceNodeMap,
        final ControllerServiceNode contextNode,
        final List<ControllerServiceNode> orderedNodes,
        final Set<ControllerServiceNode> visited) {
        if (visited.contains(contextNode)) {
            return;
        }

        for (final Map.Entry<PropertyDescriptor, String> entry : contextNode.getEffectivePropertyValues().entrySet()) {
            if (entry.getKey().getControllerServiceDefinition() != null) {
                final String referencedServiceId = entry.getValue();
                if (referencedServiceId != null) {
                    final ControllerServiceNode referencedNode = serviceNodeMap.get(referencedServiceId);
                    if (!orderedNodes.contains(referencedNode)) {
                        visited.add(contextNode);
                        determineEnablingOrder(serviceNodeMap, referencedNode, orderedNodes, visited);
                    }
                }
            }
        }

        if (!orderedNodes.contains(contextNode)) {
            orderedNodes.add(contextNode);
        }
    }

    /**
     * Waits for all given Controller Services to complete validation
     *
     * @return <code>true</code> if all processors have completed validation, <code>false</code> if the given {@link Pause}
     *         indicated to give up before all of the controller services have completed validation
     */
    private boolean waitForControllerServiceValidation(final String groupId, final Map<String, AffectedComponentEntity> affectedComponents, final Pause pause) {
        logger.debug("Waiting for {} controller services to complete validation", affectedComponents.size());
        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ControllerServiceEntity> serviceEntities = serviceFacade.getControllerServices(groupId, false, true);
            if (isControllerServiceValidationComplete(serviceEntities, affectedComponents)) {
                logger.debug("All {} controller services of interest have completed validation", affectedComponents.size());
                return true;
            } else if (logger.isDebugEnabled()) {
                final Set<ControllerServiceEntity> validating = getValidatingControllerServices(serviceEntities, affectedComponents);
                logger.debug("The following {} Controller Services still have a state of VALIDATING: {}", validating.size(), validating);
            }

            continuePolling = pause.pause();
        }
        return false;
    }

    private boolean isControllerServiceValidationComplete(final Set<ControllerServiceEntity> controllerServiceEntities, final Map<String, AffectedComponentEntity> affectedComponents) {
        updateAffectedControllerServices(controllerServiceEntities, affectedComponents);
        for (final ControllerServiceEntity entity : controllerServiceEntities) {
            if (!affectedComponents.containsKey(entity.getId())) {
                continue;
            }

            if (ControllerServiceDTO.VALIDATING.equals(entity.getComponent().getValidationStatus())) {
                return false;
            }
        }
        return true;
    }

    private Set<ControllerServiceEntity> getValidatingControllerServices(final Set<ControllerServiceEntity> controllerServiceEntities, final Map<String, AffectedComponentEntity> affectedComponents) {
        return controllerServiceEntities.stream()
            .filter(service -> affectedComponents.containsKey(service.getId()))
            .filter(service -> ControllerServiceDTO.VALIDATING.equals(service.getComponent().getValidationStatus()))
            .collect(Collectors.toSet());
    }

    /**
     * Periodically polls the process group with the given ID, waiting for all controller services whose ID's are given to have the given Controller Service State.
     *
     * @param groupId the ID of the Process Group to poll
     * @param affectedServices all Controller Services whose state should be equal to the given desired state
     * @param desiredState the desired state for all services with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for services to reach the desired state
     */
    private boolean waitForControllerServiceState(final String groupId, final Map<String, AffectedComponentEntity> affectedServices, final ControllerServiceState desiredState, final Pause pause,
                                                  final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        logger.debug("Waiting for {} Controller Services to transition their states to {}", affectedServices.size(), desiredState);

        boolean continuePolling = true;
        while (continuePolling) {
            final Set<ControllerServiceEntity> serviceEntities = serviceFacade.getControllerServices(groupId, false, true);

            // update the affected controller services
            updateAffectedControllerServices(serviceEntities, affectedServices);

            final String desiredStateName = desiredState.name();

            boolean allReachedDesiredState = true;
            for (final ControllerServiceEntity serviceEntity : serviceEntities) {
                final ControllerServiceDTO serviceDto = serviceEntity.getComponent();
                if (!affectedServices.containsKey(serviceDto.getId())) {
                    logger.debug("When checking if Controller Service has reached desired state for {}, the Controller Service is not in the set of affected components so ignoring its state",
                        serviceDto.getId());

                    continue;
                }

                final String validationStatus = serviceDto.getValidationStatus();
                if (ControllerServiceDTO.INVALID.equals(validationStatus)) {
                    logger.debug("When checking if Controller Service has reached desired state for {}, desired state = {}, validation status = {}, invalid component action = {}",
                        serviceEntity.getId(), desiredState, validationStatus, invalidComponentAction);

                    switch (invalidComponentAction) {
                        case WAIT:
                            allReachedDesiredState = false;
                            break;
                        case SKIP:
                            continue;
                        case FAIL:
                            final String action = desiredState == ControllerServiceState.ENABLED ? "enable" : "disable";
                            throw new LifecycleManagementException("Could not " + action + " " + serviceEntity.getComponent().getName() + " because it is invalid");
                    }
                }

                if (!desiredStateName.equals(serviceDto.getState())) {
                    logger.debug("When checking if Controller Service has reached desired state for {}, desired state = {}, service state = {}. This service has not reached the desired state.",
                        serviceEntity.getId(), desiredState, serviceDto.getState());

                    allReachedDesiredState = false;
                    break;
                }
            }

            if (allReachedDesiredState) {
                logger.debug("All {} controller services of interest now have the desired state of {}", affectedServices.size(), desiredState);
                return true;
            } else {
                logger.debug("Not all {} controller services have reached the desired state of {}. Will continue polling and waiting for the desired state.", affectedServices.size(), desiredState);
            }

            // Not all of the controller services are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }


    /**
     * Updates the affected controller services in the specified updateRequest with the serviceEntities.
     *
     * @param serviceEntities service entities
     * @param affectedServices all Controller Services whose state should be equal to the given desired state
     */
    private void updateAffectedControllerServices(final Set<ControllerServiceEntity> serviceEntities, final Map<String, AffectedComponentEntity> affectedServices) {
        // update the affected components
        serviceEntities.stream()
            .filter(entity -> affectedServices.containsKey(entity.getId()))
            .forEach(entity -> {
                final AffectedComponentEntity affectedComponentEntity = affectedServices.get(entity.getId());
                affectedComponentEntity.setRevision(entity.getRevision());

                // only consider update this component if the user had permissions to it
                if (Boolean.TRUE.equals(affectedComponentEntity.getPermissions().getCanRead())) {
                    final AffectedComponentDTO affectedComponent = affectedComponentEntity.getComponent();
                    affectedComponent.setState(entity.getComponent().getState());

                    if (Boolean.TRUE.equals(entity.getPermissions().getCanRead())) {
                        affectedComponent.setValidationErrors(entity.getComponent().getValidationErrors());
                    }
                }
            });
    }


    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }
}
