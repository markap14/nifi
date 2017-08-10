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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response.Status;

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.revision.RevisionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class JerseyClientComponentLifecycle implements ComponentLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(JerseyClientComponentLifecycle.class);

    private Client jerseyClient;
    private RevisionManager revisionManager;

    public JerseyClientComponentLifecycle(final NiFiProperties properties) {
        jerseyClient = WebUtils.createClient(new DefaultClientConfig(), SslContextFactory.createSslContext(properties));
        final int connectionTimeout = (int) FormatUtils.getTimeDuration(properties.getClusterNodeConnectionTimeout(), TimeUnit.MILLISECONDS);
        final int readTimeout = (int) FormatUtils.getTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
        jerseyClient.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, connectionTimeout);
        jerseyClient.getProperties().put(ClientConfig.PROPERTY_READ_TIMEOUT, readTimeout);
        jerseyClient.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, Boolean.TRUE);
    }

    @Override
    public void close() throws IOException {
        jerseyClient.destroy();
    }

    @Override
    public Set<AffectedComponentEntity> scheduleComponents(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> components, final ScheduledState desiredState,
        final Pause pause) throws LifecycleManagementException {

        // TODO: Be smart about how to handle an empty set
        // Verify the given revisions
        for (final AffectedComponentEntity componentEntity : components) {
            final RevisionDTO givenRevisionDto = componentEntity.getRevision();
            final Revision givenRevision = new Revision(givenRevisionDto.getVersion(), givenRevisionDto.getClientId(), componentEntity.getId());
            final Revision actualRevision = revisionManager.getRevision(componentEntity.getId());
            if (!givenRevision.equals(actualRevision)) {
                throw new InvalidRevisionException("The Revision given for Component with ID " + componentEntity.getId() + " is the wrong Revision");
            }
        }

        final Set<String> affectedComponentIds = components.stream()
            .map(component -> component.getComponent().getId())
            .collect(Collectors.toSet());

        final Map<String, RevisionDTO> componentRevisionDtoMap = components.stream()
            .collect(Collectors.toMap(entity -> entity.getComponent().getId(), AffectedComponentEntity::getRevision));


        final ScheduleComponentsEntity scheduleComponentsEntity = new ScheduleComponentsEntity();
        scheduleComponentsEntity.setComponents(componentRevisionDtoMap);
        scheduleComponentsEntity.setId(groupId);
        scheduleComponentsEntity.setState(desiredState.name());

        URI scheduleComponentUri;
        try {
            scheduleComponentUri = new URI(exampleUri.getScheme(), exampleUri.getUserInfo(), exampleUri.getHost(),
                exampleUri.getPort(), "/nifi-api/flow/process-groups/" + groupId, null, exampleUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final ClientResponse scheduleComponentsResponse = jerseyClient.resource(scheduleComponentUri)
            .header("Content-Type", "application/json")
            .entity(scheduleComponentsEntity)
            .put(ClientResponse.class);

        final int scheduleComponentsStatus = scheduleComponentsResponse.getStatus();
        if (scheduleComponentsStatus != Status.OK.getStatusCode()) {
            throw new LifecycleManagementException("Failed to change components' states to " + desiredState
                + ": Received HTTP status code " + scheduleComponentsStatus + ": " + scheduleComponentsResponse.getEntity(String.class));
        }

        final boolean componentsTransitioned = waitForProcessorStatus(jerseyClient, exampleUri, groupId, affectedComponentIds, desiredState, pause);
        if (!componentsTransitioned) {
            throw new LifecycleManagementException("Failed while waiting for all nodes to change state of components to " + desiredState);
        }

        // TODO: Need to get new Revisions.
        return components;
    }

    @Override
    public Set<AffectedComponentEntity> activateControllerServices(final URI exampleUri, final String groupId, final Set<AffectedComponentEntity> services,
        final ControllerServiceState desiredState, final Pause pause) throws LifecycleManagementException {

        // TODO: Be smart about how to handle an empty set
        // Verify the given revisions
        for (final AffectedComponentEntity serviceEntity : services) {
            final RevisionDTO givenRevisionDto = serviceEntity.getRevision();
            final Revision givenRevision = new Revision(givenRevisionDto.getVersion(), givenRevisionDto.getClientId(), serviceEntity.getId());
            final Revision actualRevision = revisionManager.getRevision(serviceEntity.getId());
            if (!givenRevision.equals(actualRevision)) {
                throw new InvalidRevisionException("The Revision given for Controller Service with ID " + serviceEntity.getId() + " is the wrong Revision");
            }
        }

        final Set<String> affectedServiceIds = services.stream()
            .map(component -> component.getComponent().getId())
            .collect(Collectors.toSet());

        final Map<String, RevisionDTO> serviceRevisionDtoMap = services.stream()
            .collect(Collectors.toMap(entity -> entity.getComponent().getId(), AffectedComponentEntity::getRevision));

        final ActivateControllerServicesEntity activateServicesEntity = new ActivateControllerServicesEntity();
        activateServicesEntity.setComponents(serviceRevisionDtoMap);
        activateServicesEntity.setId(groupId);
        activateServicesEntity.setState(desiredState.name());

        URI disableServicesUri;
        try {
            disableServicesUri = new URI(exampleUri.getScheme(), exampleUri.getUserInfo(), exampleUri.getHost(),
                exampleUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", null, exampleUri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        final ClientResponse disableServicesResponse;
        try {
            disableServicesResponse = jerseyClient.resource(disableServicesUri)
                .header("Content-Type", "application/json")
                .entity(activateServicesEntity)
                .put(ClientResponse.class);
        } catch (ClientHandlerException | UniformInterfaceException e) {
            throw new LifecycleManagementException(e);
        }

        final int disableServicesStatus = disableServicesResponse.getStatus();
        if (disableServicesStatus != Status.OK.getStatusCode()) {
            throw new LifecycleManagementException("Failed to change Controller Service state to " + desiredState
                + ": Received HTTP status code " + disableServicesStatus + ": " + disableServicesResponse.getEntity(String.class));
        }

        if (!waitForControllerServiceStatus(jerseyClient, exampleUri, groupId, affectedServiceIds, desiredState, pause)) {
            throw new LifecycleManagementException("Failed while waiting for all nodes to change state of Controller Services to " + desiredState);
        }

        // TODO: Need to get new Revisions.
        return services;
    }


    /**
     * Periodically polls the process group with the given ID, waiting for all processors whose ID's are given to have the given Scheduled State.
     *
     * @param client the Jersey Client to use for making the request
     * @param groupId the ID of the Process Group to poll
     * @param processorIds the ID of all Processors whose state should be equal to the given desired state
     * @param desiredState the desired state for all processors with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for processors to reach the desired state
     */
    private boolean waitForProcessorStatus(final Client client, final URI originalUri, final String groupId, final Set<String> processorIds, final ScheduledState desiredState, final Pause pause) {
        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/status", "recursive=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        boolean continuePolling = true;
        while (continuePolling) {
            final ClientResponse response = client.resource(groupUri).header("Content-Type", "application/json").get(ClientResponse.class);
            if (response.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ProcessGroupStatusEntity statusEntity = response.getEntity(ProcessGroupStatusEntity.class);
            final ProcessGroupStatusDTO statusDto = statusEntity.getProcessGroupStatus();
            final ProcessGroupStatusSnapshotDTO statusSnapshotDto = statusDto.getAggregateSnapshot();

            if (isProcessorStatusEqual(statusSnapshotDto, processorIds, desiredState)) {
                logger.debug("All {} processors of interest now have the desired state of {}", processorIds.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }

    private boolean isProcessorStatusEqual(final ProcessGroupStatusSnapshotDTO statusSnapshot, final Set<String> processorIds, final ScheduledState desiredState) {
        final String desiredStateName = desiredState.name();

        final boolean allProcessorsMatch = statusSnapshot.getProcessorStatusSnapshots().stream()
            .map(entity -> entity.getProcessorStatusSnapshot())
            .filter(status -> processorIds.contains(status.getId()))
            .allMatch(status -> {
                final String runStatus = status.getRunStatus();
                final boolean stateMatches = desiredStateName.equalsIgnoreCase(runStatus);
                if (!stateMatches) {
                    return false;
                }

                if (desiredState == ScheduledState.STOPPED && status.getActiveThreadCount() != 0) {
                    return false;
                }

                return true;
            });

        if (!allProcessorsMatch) {
            return false;
        }

        for (final ProcessGroupStatusSnapshotEntity childGroupEntity : statusSnapshot.getProcessGroupStatusSnapshots()) {
            final ProcessGroupStatusSnapshotDTO childGroupStatus = childGroupEntity.getProcessGroupStatusSnapshot();
            final boolean allMatchChildLevel = isProcessorStatusEqual(childGroupStatus, processorIds, desiredState);
            if (!allMatchChildLevel) {
                return false;
            }
        }

        return true;
    }


    /**
     * Periodically polls the process group with the given ID, waiting for all controller services whose ID's are given to have the given Controller Service State.
     *
     * @param client the Jersey Client to use for making the HTTP Request
     * @param groupId the ID of the Process Group to poll
     * @param serviceIds the ID of all Controller Services whose state should be equal to the given desired state
     * @param desiredState the desired state for all services with the ID's given
     * @param pause the Pause that can be used to wait between polling
     * @return <code>true</code> if successful, <code>false</code> if unable to wait for services to reach the desired state
     */
    private boolean waitForControllerServiceStatus(final Client client, final URI originalUri, final String groupId, final Set<String> serviceIds, final ControllerServiceState desiredState,
        final Pause pause) throws LifecycleManagementException {

        URI groupUri;
        try {
            groupUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/flow/process-groups/" + groupId + "/controller-services", "includeAncestorGroups=false,includeDescendantGroups=true", originalUri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        boolean continuePolling = true;
        while (continuePolling) {
            final ClientResponse response;

            try {
                response = client.resource(groupUri).header("Content-Type", "application/json").get(ClientResponse.class);
            } catch (ClientHandlerException | UniformInterfaceException e) {
                throw new LifecycleManagementException(e);
            }

            if (response.getStatus() != Status.OK.getStatusCode()) {
                return false;
            }

            final ControllerServicesEntity controllerServicesEntity = response.getEntity(ControllerServicesEntity.class);
            final Set<ControllerServiceEntity> serviceEntities = controllerServicesEntity.getControllerServices();

            final String desiredStateName = desiredState.name();
            final boolean allServicesMatch = serviceEntities.stream()
                .map(entity -> entity.getComponent())
                .filter(service -> serviceIds.contains(service.getId()))
                .map(service -> service.getState())
                .allMatch(state -> state.equals(desiredStateName));

            if (allServicesMatch) {
                logger.debug("All {} controller services of interest now have the desired state of {}", serviceIds.size(), desiredState);
                return true;
            }

            // Not all of the processors are in the desired state. Pause for a bit and poll again.
            continuePolling = pause.pause();
        }

        return false;
    }


    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }
}
