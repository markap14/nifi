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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.ApplicationResource.ReplicationTarget;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterReplicationComponentLifecycle implements ComponentLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(ClusterReplicationComponentLifecycle.class);

    private ClusterCoordinator clusterCoordinator;
    private RequestReplicator requestReplicator;
    private NiFiServiceFacade serviceFacade;
    private DtoFactory dtoFactory;


    @Override
    public Set<AffectedComponentEntity> scheduleComponents(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> components,
            final ScheduledState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        final Set<String> componentIds = components.stream()
            .map(ComponentEntity::getId)
            .collect(Collectors.toSet());

        final Map<String, AffectedComponentEntity> componentMap = components.stream()
            .collect(Collectors.toMap(AffectedComponentEntity::getId, Function.identity()));

        final Map<String, Revision> componentRevisionMap = getRevisions(groupId, componentIds);
        final Map<String, RevisionDTO> componentRevisionDtoMap = componentRevisionMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> dtoFactory.createRevisionDTO(entry.getValue())));

        final ScheduleComponentsEntity scheduleProcessorsEntity = new ScheduleComponentsEntity();
        scheduleProcessorsEntity.setComponents(componentRevisionDtoMap);
        scheduleProcessorsEntity.setId(groupId);
        scheduleProcessorsEntity.setState(desiredState.name());

        logger.debug("Scheduling {} components to have a state of {}: {}", componentRevisionDtoMap.size(), desiredState, componentRevisionDtoMap.keySet());

        URI scheduleGroupUri;
        try {
            scheduleGroupUri = new URI(exampleUri.getScheme(), exampleUri.getUserInfo(), exampleUri.getHost(),
                exampleUri.getPort(), "/nifi-api/flow/process-groups/" + groupId, null, exampleUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // If attempting to run the processors, validation must complete first
        if (desiredState == ScheduledState.RUNNING) {
            try {
                waitForProcessorValidation(user, exampleUri, groupId, componentMap, pause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LifecycleManagementException("Interrupted while waiting for processors to complete validation");
            }
        }

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
        try {
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.PUT, scheduleGroupUri, scheduleProcessorsEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), user, HttpMethod.PUT, scheduleGroupUri, scheduleProcessorsEntity, headers).awaitMergedResponse();
            }

            final int scheduleComponentStatus = clusterResponse.getStatus();
            if (scheduleComponentStatus != Status.OK.getStatusCode()) {
                final String explanation = getResponseEntity(clusterResponse, String.class);
                throw new LifecycleManagementException("Failed to transition components to a state of " + desiredState + " due to " + explanation);
            }

            final boolean processorsTransitioned = waitForProcessorStatus(user, exampleUri, groupId, componentMap, desiredState, pause, invalidComponentAction);

            if (!processorsTransitioned) {
                throw new LifecycleManagementException("Failed while waiting for components to transition to state of " + desiredState);
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new LifecycleManagementException("Interrupted while attempting to transition components to state of " + desiredState);
        }

        logger.debug("Successfully transitioned {} components to a state of {}", components.size(), desiredState);
        final Set<AffectedComponentEntity> updatedEntities = components.stream()
            .map(component -> AffectedComponentUtils.updateEntity(component, serviceFacade, dtoFactory))
            .collect(Collectors.toSet());
        return updatedEntities;
    }


    private ReplicationTarget getReplicationTarget() {
        return clusterCoordinator.isActiveClusterCoordinator() ? ReplicationTarget.CLUSTER_NODES : ReplicationTarget.CLUSTER_COORDINATOR;
    }

    private RequestReplicator getRequestReplicator() {
        return requestReplicator;
    }

    protected NodeIdentifier getClusterCoordinatorNode() {
        final NodeIdentifier activeClusterCoordinator = clusterCoordinator.getElectedActiveCoordinatorNode();
        if (activeClusterCoordinator != null) {
            return activeClusterCoordinator;
        }

        throw new NoClusterCoordinatorException();
    }

    private Map<String, Revision> getRevisions(final String groupId, final Set<String> componentIds) {
        final Set<Revision> processorRevisions = serviceFacade.getRevisionsFromGroup(groupId, group -> componentIds);
        return processorRevisions.stream().collect(Collectors.toMap(Revision::getComponentId, Function.identity()));
    }

    private boolean waitForProcessorValidation(final NiFiUser user, final URI originalUri, final String groupId,
                                               final Map<String, AffectedComponentEntity> processors, final Pause pause) throws InterruptedException {
        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                    originalUri.getPort(), "/nifi-api/process-groups/" + groupId + "/processors", "includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();

        boolean continuePolling = true;
        while (continuePolling) {

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            }

            if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ProcessorsEntity processorsEntity = getResponseEntity(clusterResponse, ProcessorsEntity.class);
            final Set<ProcessorEntity> processorEntities = processorsEntity.getProcessors();

            if (isProcessorValidationComplete(processorEntities, processors)) {
                logger.debug("All {} processors of interest now have been validated", processors.size());
                return true;
            } else if (logger.isDebugEnabled()) {
                final Set<ProcessorEntity> validating = getValidatingProcessors(processorEntities, processors);
                logger.debug("The following {} Processors still have a state of VALIDATING: {}", validating.size(), validating);
            }

            // Not all of the processors are done validating. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    private boolean isProcessorValidationComplete(Set<ProcessorEntity> processorEntities, Map<String, AffectedComponentEntity> affectedComponents) {
        updateAffectedProcessors(processorEntities, affectedComponents);
        for (final ProcessorEntity entity : processorEntities) {
            if (!affectedComponents.containsKey(entity.getId())) {
                continue;
            }

            if (ProcessorDTO.VALIDATING.equals(entity.getComponent().getValidationStatus())) {
                return false;
            }
        }
        return true;
    }

    private Set<ProcessorEntity> getValidatingProcessors(final Set<ProcessorEntity> processorEntities, final Map<String, AffectedComponentEntity> affectedComponents) {
        return processorEntities.stream()
            .filter(proc -> affectedComponents.containsKey(proc.getId()))
            .filter(proc -> ProcessorDTO.VALIDATING.equals(proc.getComponent().getValidationStatus()))
            .collect(Collectors.toSet());
    }

    /**
     * Periodically polls the process group with the given ID, waiting for all processors whose ID's are given to have the given Scheduled State.
     *
     * @param user the user making the request
     * @param originalUri the original uri
     * @param groupId the ID of the Process Group to poll
     * @param processors the Processors whose state should be equal to the given desired state
     * @param desiredState the desired state for all processors with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @param invalidComponentAction indicates how to handle the condition of a Processor being invalid
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for processors to reach the desired state
     */
    private boolean waitForProcessorStatus(final NiFiUser user, final URI originalUri, final String groupId, final Map<String, AffectedComponentEntity> processors,
                final ScheduledState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction) throws InterruptedException, LifecycleManagementException {

        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/process-groups/" + groupId + "/processors", "includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();

        int loopCount = 0;
        boolean continuePolling = true;
        while (continuePolling) {
            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            }

            if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ProcessorsEntity processorsEntity = getResponseEntity(clusterResponse, ProcessorsEntity.class);
            final Set<ProcessorEntity> processorEntities = processorsEntity.getProcessors();

            final boolean debugLog = loopCount++ % 10 == 0; // Log debug info every 10th iteration. This is typically called with a Pause of 250 ms, so don't want to log 4x per second.
            if (isProcessorActionComplete(processorEntities, processors, desiredState, invalidComponentAction, debugLog)) {
                logger.debug("All {} processors of interest now have the desired state of {}", processors.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    /**
     * Extracts the response entity from the specified node response.
     *
     * @param nodeResponse node response
     * @param clazz class
     * @param <T> type of class
     * @return the response entity
     */
    @SuppressWarnings("unchecked")
    private <T> T getResponseEntity(final NodeResponse nodeResponse, final Class<T> clazz) {
        T entity = (T) nodeResponse.getUpdatedEntity();
        if (entity == null) {
            entity = nodeResponse.getClientResponse().readEntity(clazz);
        }
        return entity;
    }

    private void updateAffectedProcessors(final Set<ProcessorEntity> processorEntities, final Map<String, AffectedComponentEntity> affectedComponents) {
        // update the affected processors
        processorEntities.stream()
                .filter(entity -> affectedComponents.containsKey(entity.getId()))
                .forEach(entity -> {
                    final AffectedComponentEntity affectedComponentEntity = affectedComponents.get(entity.getId());
                    affectedComponentEntity.setRevision(entity.getRevision());

                    // only consider update this component if the user had permissions to it
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


    @Override
    public Set<AffectedComponentEntity> activateControllerServices(final URI originalUri, final String groupId, final Set<AffectedComponentEntity> affectedServices,
                                        final ControllerServiceState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction) throws LifecycleManagementException {

        final Set<String> affectedServiceIds = affectedServices.stream()
            .map(ComponentEntity::getId)
            .collect(Collectors.toSet());

        final Map<String, Revision> serviceRevisionMap = getRevisions(groupId, affectedServiceIds);
        final Map<String, RevisionDTO> serviceRevisionDtoMap = serviceRevisionMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> dtoFactory.createRevisionDTO(entry.getValue())));

        final ActivateControllerServicesEntity activateServicesEntity = new ActivateControllerServicesEntity();
        activateServicesEntity.setComponents(serviceRevisionDtoMap);
        activateServicesEntity.setId(groupId);
        activateServicesEntity.setState(desiredState.name());

        URI controllerServicesUri;
        try {
            controllerServicesUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", null, originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // If enabling services, validation must complete first
        if (desiredState == ControllerServiceState.ENABLED) {
            try {
                waitForControllerServiceValidation(user, originalUri, groupId, affectedServiceIds, pause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LifecycleManagementException("Interrupted while waiting for Controller Services to complete validation");
            }
        }

        // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
        try {
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.PUT, controllerServicesUri, activateServicesEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), user, HttpMethod.PUT, controllerServicesUri, activateServicesEntity, headers).awaitMergedResponse();
            }

            final int disableServicesStatus = clusterResponse.getStatus();
            if (disableServicesStatus != Status.OK.getStatusCode()) {
                final String explanation = getResponseEntity(clusterResponse, String.class);
                throw new LifecycleManagementException("Failed to update Controller Services to a state of " + desiredState + " due to " + explanation);
            }

            final boolean serviceTransitioned = waitForControllerServiceStatus(user, originalUri, groupId, affectedServiceIds, desiredState, pause, invalidComponentAction);

            if (!serviceTransitioned) {
                throw new LifecycleManagementException("Failed while waiting for Controller Services to finish transitioning to a state of " + desiredState);
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new LifecycleManagementException("Interrupted while transitioning Controller Services to a state of " + desiredState);
        }

        logger.debug("Successfully transitioned {} Controller Services to a state of {}", affectedServices.size(), desiredState);

        return affectedServices.stream()
            .map(componentEntity -> serviceFacade.getControllerService(componentEntity.getId()))
            .map(dtoFactory::createAffectedComponentEntity)
            .collect(Collectors.toSet());
    }

    private boolean waitForControllerServiceValidation(final NiFiUser user, final URI originalUri, final String groupId,
                                                       final Set<String> serviceIds, final Pause pause)
            throws InterruptedException {

        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                    originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", "includeAncestorGroups=false,includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();

        boolean continuePolling = true;
        while (continuePolling) {

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            }

            if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ControllerServicesEntity controllerServicesEntity = getResponseEntity(clusterResponse, ControllerServicesEntity.class);
            final Set<ControllerServiceEntity> serviceEntities = controllerServicesEntity.getControllerServices();

            final Map<String, AffectedComponentEntity> affectedServices = serviceEntities.stream()
                    .collect(Collectors.toMap(ControllerServiceEntity::getId, dtoFactory::createAffectedComponentEntity));

            if (isControllerServiceValidationComplete(serviceEntities, affectedServices)) {
                logger.debug("All {} controller services of interest have completed validation", affectedServices.size());
                return true;
            } else if (logger.isDebugEnabled()) {
                final Set<ControllerServiceEntity> validating = getValidatingControllerServices(serviceEntities, affectedServices);
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
     * @param user the user making the request
     * @param groupId the ID of the Process Group to poll
     * @param serviceIds the ID of all Controller Services whose state should be equal to the given desired state
     * @param desiredState the desired state for all services with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for services to reach the desired state
     */
    private boolean waitForControllerServiceStatus(final NiFiUser user, final URI originalUri, final String groupId, final Set<String> serviceIds,
                                                   final ControllerServiceState desiredState, final Pause pause, final InvalidComponentAction invalidComponentAction)
        throws InterruptedException, LifecycleManagementException {

        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", "includeAncestorGroups=false,includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final Map<String, String> headers = new HashMap<>();
        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();

        boolean continuePolling = true;
        while (continuePolling) {

            // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly to the cluster nodes themselves.
            final NodeResponse clusterResponse;
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                    getClusterCoordinatorNode(), user, HttpMethod.GET, groupUri, requestEntity, headers).awaitMergedResponse();
            }

            if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ControllerServicesEntity controllerServicesEntity = getResponseEntity(clusterResponse, ControllerServicesEntity.class);
            final Set<ControllerServiceEntity> serviceEntities = controllerServicesEntity.getControllerServices();

            final Map<String, AffectedComponentEntity> affectedServices = serviceEntities.stream()
                .collect(Collectors.toMap(ControllerServiceEntity::getId, dtoFactory::createAffectedComponentEntity));

            // update the affected controller services
            updateAffectedControllerServices(serviceEntities, affectedServices);

            final String desiredStateName = desiredState.name();
            boolean allReachedDesiredState = true;
            for (final ControllerServiceEntity serviceEntity : serviceEntities) {
                final ControllerServiceDTO serviceDto = serviceEntity.getComponent();
                if (serviceDto == null) {
                    continue;
                }

                if (!serviceIds.contains(serviceDto.getId())) {
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

                if (!desiredStateName.equalsIgnoreCase(serviceDto.getState())) {
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
     * @param affectedServices affected services
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

    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    public void setRequestReplicator(final RequestReplicator requestReplicator) {
        this.requestReplicator = requestReplicator;
    }

    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }
}
