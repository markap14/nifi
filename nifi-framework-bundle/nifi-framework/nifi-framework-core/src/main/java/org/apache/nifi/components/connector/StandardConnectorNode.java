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

package org.apache.nifi.components.connector;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.FlowFileActivity;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StandardConnectorNode implements ConnectorNode {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorNode.class);

    private final String identifier;
    private final ExtensionManager extensionManager;
    private final Authorizable parentAuthorizable;
    private final ProcessGroup managedProcessGroup;
    private final ConnectorDetails connectorDetails;
    private final String componentType;
    private final BundleCoordinate bundleCoordinate;
    private final StandardConnectorConfigurationContext configurationContext;
    private final ConnectorStateTransition stateTransition;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final AtomicReference<ConnectorState> updateResumeState = new AtomicReference<>(null);

    private volatile String name;
    private volatile boolean performValidation = true;


    public StandardConnectorNode(final String identifier, final ExtensionManager extensionManager, final Authorizable parentAuthorizable, final ProcessGroup managedProcessGroup,
        final ConnectorDetails connectorDetails, final String componentType, final BundleCoordinate bundleCoordinate,
        final StandardConnectorConfigurationContext configurationContext, final ConnectorStateTransition stateTransition) {

        this.identifier = identifier;
        this.extensionManager = extensionManager;
        this.parentAuthorizable = parentAuthorizable;
        this.managedProcessGroup = managedProcessGroup;
        this.connectorDetails = connectorDetails;
        this.componentType = componentType;
        this.bundleCoordinate = bundleCoordinate;
        this.configurationContext = configurationContext;
        this.stateTransition = stateTransition;
        this.name = connectorDetails.getConnector().getClass().getSimpleName();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public ConnectorConfiguration getConfiguration() {
        return configurationContext.toConnectorConfiguration();
    }

    @Override
    public void prepareForUpdate(final FlowEngine scheduler) throws FlowUpdateException {
        final ConnectorState initialState = getCurrentState();
        if (initialState == ConnectorState.UPDATING || initialState == ConnectorState.PREPARING_FOR_UPDATE) {
            return;
        }

        updateResumeState.set(initialState);
        stateTransition.setDesiredState(ConnectorState.UPDATING);
        stateTransition.setCurrentState(ConnectorState.PREPARING_FOR_UPDATE);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().prepareForUpdate();
            stateTransition.setCurrentState(ConnectorState.UPDATING);
        } catch (final Throwable t) {
            logger.error("Failed to prepare update for {}", this, t);
            stateTransition.setCurrentState(ConnectorState.UPDATE_FAILED);

            try {
                getConnector().abortUpdatePreparation(t);
            } catch (final Throwable abortFailure) {
                logger.error("Failed to abort update preparation for {}", this, abortFailure);
            }

            throw t;
        }
    }

    @Override
    public void finishUpdate(final FlowEngine scheduler) throws FlowUpdateException {
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot finish update for " + this + " because its state is currently " + currentState
                                            + "; it must be UPDATING.");
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().finishUpdate();
        } catch (final Throwable t) {
            logger.error("Failed to finish update for {}", this, t);
            stateTransition.setCurrentState(ConnectorState.UPDATE_FAILED);
            stateTransition.setDesiredState(ConnectorState.UPDATE_FAILED);

            throw new FlowUpdateException("Failed to finish update for " + this, t);
        }

        final ConnectorState stateToResume = updateResumeState.getAndSet(null);
        if (stateToResume == ConnectorState.DISABLED) {
            disable();
        } else if (stateToResume == ConnectorState.STOPPED) {
            stop(scheduler);
        } else if (stateToResume == ConnectorState.RUNNING) {
            start(scheduler);
        }
    }

    @Override
    public void abortUpdate(final Throwable cause) {
        stateTransition.setCurrentState(ConnectorState.UPDATE_FAILED);
        stateTransition.setDesiredState(ConnectorState.UPDATE_FAILED);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().abortUpdatePreparation(cause);
        }
    }

    @Override
    public void setConfiguration(final String stepName, final List<PropertyGroupConfiguration> groupConfigurations) throws FlowUpdateException {
        // Ensure that the Connector is fully stopped before allowing configuration to be updated
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot update the configuration of " + this + " because its state is currently " + currentState
                                            + "; its state must be UPDATING in order to configure it.");
        }

        // Desired State must also be UPDATING to ensure that the Connector is not transitioning to a new state during the configuration change
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot update the configuration of " + this + " because its desired state is currently " + desiredState
                                            + "; its state must be UPDATING in order to configure it.");
        }

        // Determine which configuration steps will change as a result of applying this new configuration
        final ConfigurationUpdateResult updateResult = configurationContext.setProperties(stepName, groupConfigurations);

        if (updateResult == ConfigurationUpdateResult.NO_CHANGES) {
            return;
        }

        final Connector connector = connectorDetails.getConnector();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connector.getClass(), getIdentifier())) {
            logger.debug("Notifying {} of configuration change for configuration step {}", this, stepName);
            connector.onConfigurationStepConfigured(stepName);
        } catch (final FlowUpdateException e) {
            throw e;
        } catch (final Exception e) {
            logger.error("Failed to invoke onConfigured for {}", this, e);
            throw new RuntimeException("Failed to invoke onConfigured for " + this, e);
        }
    }

    @Override
    public ConnectorState getCurrentState() {
        return stateTransition.getCurrentState();
    }

    @Override
    public ConnectorState getDesiredState() {
        return stateTransition.getDesiredState();
    }

    @Override
    public void enable() {
        stateTransition.setDesiredState(ConnectorState.STOPPED);
        if (stateTransition.trySetCurrentState(ConnectorState.DISABLED, ConnectorState.STOPPED)) {
            logger.info("Transitioned current state for {} to {}", this, ConnectorState.STOPPED);
            return;
        }

        logger.info("{} enabled but not currently DISABLED so set desired state to STOPPED; current state is {}", this, stateTransition.getCurrentState());
    }

    @Override
    public void disable() {
        stateTransition.setDesiredState(ConnectorState.DISABLED);

        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.DISABLED || currentState == ConnectorState.STOPPED || currentState == ConnectorState.UPDATE_FAILED) {
            if (stateTransition.trySetCurrentState(currentState, ConnectorState.DISABLED)) {
                logger.info("Transitioned current state for {} to {}", this, ConnectorState.DISABLED);
                return;
            }
        }

        logger.info("{} disabled but not in a state that can immediately transition to DISABLED so set desired state to DISABLED; current state is {}", this, currentState);
    }

    @Override
    public Optional<Duration> getIdleDuration() {
        final FlowFileActivity activity = getManagedProcessGroup().getFlowFileActivity();
        final OptionalLong lastActivityTimestamp = activity.getLatestActivityTime();
        if (lastActivityTimestamp.isEmpty()) {
            return Optional.empty();
        }

        if (getManagedProcessGroup().isDataQueued()) {
            return Optional.empty();
        }

        final Duration idleDuration = Duration.ofMillis(System.currentTimeMillis() - lastActivityTimestamp.getAsLong());
        return Optional.of(idleDuration);
    }

    @Override
    public FlowFileTransferCounts getFlowFileTransferCounts() {
        return getManagedProcessGroup().getFlowFileActivity().getTransferCounts();
    }

    @Override
    public Future<Void> start(final FlowEngine scheduler) {
        final CompletableFuture<Void> startCompleteFuture = new CompletableFuture<>();
        start(scheduler, startCompleteFuture);
        return startCompleteFuture;
    }

    private void start(final FlowEngine scheduler, final CompletableFuture<Void> startCompleteFuture) {
        verifyCanStart();

        stateTransition.setDesiredState(ConnectorState.RUNNING);
        final ConnectorState currentState = getCurrentState();

        switch (currentState) {
            case STARTING -> {
                logger.debug("{} is already starting; adding future to pending start futures", this);
                stateTransition.addPendingStartFuture(startCompleteFuture);
            }
            case RUNNING -> {
                logger.debug("{} is already {}; will not attempt to start", this, currentState);
                startCompleteFuture.complete(null);
            }
            case STOPPING -> {
                // We have set the Desired State to RUNNING so when the Connector fully stops, it will be started again automatically
                logger.info("{} is currently stopping so will not trigger Connector to start until it has fully stopped", this);
                stateTransition.addPendingStartFuture(startCompleteFuture);
            }
            case STOPPED, PREPARING_FOR_UPDATE -> {
                stateTransition.setCurrentState(ConnectorState.STARTING);
                scheduler.schedule(() -> startComponent(scheduler, startCompleteFuture), 0, TimeUnit.SECONDS);
            }
            default -> {
                logger.warn("{} is in state {} and cannot be started", this, currentState);
                stateTransition.addPendingStartFuture(startCompleteFuture);
            }
        }
    }

    @Override
    public Future<Void> stop(final FlowEngine scheduler) {
        final CompletableFuture<Void> stopCompleteFuture = new CompletableFuture<>();

        stateTransition.setDesiredState(ConnectorState.STOPPED);

        boolean stateUpdated = false;
        while (!stateUpdated) {
            final ConnectorState currentState = getCurrentState();
            if (currentState == ConnectorState.STOPPED || currentState == ConnectorState.DISABLED) {
                logger.debug("{} is already {}; will not attempt to stop", this, currentState);
                stopCompleteFuture.complete(null);
                return stopCompleteFuture;
            }

            if (currentState == ConnectorState.STOPPING) {
                logger.debug("{} is already stopping; adding future to pending stop futures", this);
                stateTransition.addPendingStopFuture(stopCompleteFuture);
                return stopCompleteFuture;
            }

            stateUpdated = stateTransition.trySetCurrentState(currentState, ConnectorState.STOPPING);
        }

        scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 0, TimeUnit.SECONDS);

        return stopCompleteFuture;
    }

    private void stopComponent(final FlowEngine scheduler, final CompletableFuture<Void> stopCompleteFuture) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            connectorDetails.getConnector().stop();
        } catch (final Exception e) {
            logger.error("Failed to stop {}. Will try again in 10 seconds", this, e);
            scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 10, TimeUnit.SECONDS);
            return;
        }

        stateTransition.setCurrentState(ConnectorState.STOPPED);
        stopCompleteFuture.complete(null);

        final ConnectorState desiredState = getDesiredState();
        switch (desiredState) {
            case DISABLED -> {
                logger.info("{} was requested to be DISABLED while it was stopping so will now transition to DISABLED", this);
                disable();
            }
            case RUNNING -> {
                logger.info("{} was requested to be RUNNING while it was stopping so will attempt to start again", this);
                start(scheduler, new CompletableFuture<>());
            }
            default -> {
                // No action needed for other states
            }
        }
    }

    private void startComponent(final ScheduledExecutorService scheduler, final CompletableFuture<Void> startCompleteFuture) {
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.RUNNING) {
            logger.info("Will not start {} because the desired state is no longer RUNNING but is now {}", this, desiredState);
            return;
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            connectorDetails.getConnector().start();
        } catch (final Exception e) {
            logger.error("Failed to start {}. Will try again in 10 seconds", this, e);
            scheduler.schedule(() -> startComponent(scheduler, startCompleteFuture), 10, TimeUnit.SECONDS);
            return;
        }

        stateTransition.setCurrentState(ConnectorState.RUNNING);
        startCompleteFuture.complete(null);
    }


    @Override
    public void verifyCanDelete() {
        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.STOPPED || currentState == ConnectorState.DISABLED) {
            return;
        }

        throw new IllegalStateException("Cannot delete " + this + " because its state is currently " + currentState + "; it must be stopped before it can be deleted.");
    }

    @Override
    public void verifyCanStart() {
        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.DISABLED) {
            throw new IllegalStateException("Cannot start " + this + " because its state is currently " + currentState + "; it must be fully stopped before it can be started.");
        }

        // TODO: Instead of throwing IllegalStateException here, we should behave more like Controller Services / Processors
        //       and keep trying to start until it becomes valid or is stopped.
        final ValidationState state = performValidation();
        if (state.getStatus() != ValidationStatus.VALID) {
            throw new IllegalStateException("Cannot start " + this + " because it is not valid: " + state.getValidationErrors());
        }
    }

    @Override
    public Connector getConnector() {
        return connectorDetails.getConnector();
    }

    @Override
    public String getComponentType() {
        return componentType;
    }

    @Override
    public ProcessGroup getManagedProcessGroup() {
        return managedProcessGroup;
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().fetchAllowableValues(stepName, groupName, propertyName);
        }
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName, final String filter) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().fetchAllowableValues(stepName, groupName, propertyName, filter);
        }
    }

    @Override
    public void pauseValidationTrigger() {
        performValidation = false;
    }

    @Override
    public void resumeValidationTrigger() {
        performValidation = true;

        logger.debug("Resuming Triggering of Validation State for {}; Resetting validation state", this);
        resetValidationState();
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final List<PropertyGroupConfiguration> groupConfigurations) {
        verifyCanValidate();

        final Map<String, String> properties = new HashMap<>();
        for (final PropertyGroupConfiguration groupConfiguration : groupConfigurations) {
            properties.putAll(groupConfiguration.getPropertyValues());
        }

        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().verifyConfigurationStep(stepName, properties);
        }
    }

    private void verifyCanValidate() {
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot validate the configuration step of " + this + " because its state is currently " + currentState
                                            + "; its state must be UPDATING in order to validate a configuration step.");
        }

        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot validate the configuration step of " + this + " because its desired state is currently " + desiredState
                                            + "; its state must be UPDATING in order to validate a configuration step.");
        }
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public ComponentLog getComponentLog() {
        return connectorDetails.getComponentLog();
    }

    @Override
    public ConnectorConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().getConfigurationSteps();
        }
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return parentAuthorizable;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Connector, getIdentifier(), getName());
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.ofNullable(versionedComponentId.get());
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
        boolean updated = false;
        while (!updated) {
            final String currentId = this.versionedComponentId.get();

            if (currentId == null) {
                updated = this.versionedComponentId.compareAndSet(null, versionedComponentId);
            } else if (currentId.equals(versionedComponentId)) {
                return;
            } else if (versionedComponentId == null) {
                updated = this.versionedComponentId.compareAndSet(currentId, null);
            } else {
                throw new IllegalStateException(this + " is already under version control");
            }
        }
    }

    private void resetValidationState() {
        // TODO: Implement
    }

    @Override
    public ValidationState performValidation() {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            final List<ValidationResult> allResults = getConnector().validate();
            if (allResults == null) {
                return new ValidationState(ValidationStatus.VALID, Collections.emptyList());
            }

            // Filter out any results that are 'valid' and any results that are invalid due to the fact that a Controller Service is disabled,
            // since these will not be relevant when started.
            final List<ValidationResult> relevantResults = allResults.stream()
                .filter(result -> !result.isValid())
                .filter(result -> !DisabledServiceValidationResult.isMatch(result))
                .toList();

            if (relevantResults.isEmpty()) {
                return new ValidationState(ValidationStatus.VALID, Collections.emptyList());
            }

            return new ValidationState(ValidationStatus.INVALID, relevantResults);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StandardConnectorNode that = (StandardConnectorNode) o;
        return Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identifier);
    }

    @Override
    public String toString() {
        return "StandardConnectorNode[id=" + identifier + ", name=" + name + ", state=" + stateTransition.getCurrentState() + "]";
    }
}
