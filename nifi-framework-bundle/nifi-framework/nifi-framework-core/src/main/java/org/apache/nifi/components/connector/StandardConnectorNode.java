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
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardConnectorNode implements ConnectorNode {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorNode.class);

    private final String identifier;
    private final ExtensionManager extensionManager;
    private final Authorizable parentAuthorizable;
    private final ProcessGroup managedProcessGroup;
    private final ConnectorDetails connectorDetails;
    private final String componentType;
    private final BundleCoordinate bundleCoordinate;
    private final ConnectorConfigurationContext configurationContext;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final AtomicReference<ConnectorState> currentState = new AtomicReference<>(ConnectorState.STOPPED);
    private final AtomicReference<ConnectorState> desiredState = new AtomicReference<>(ConnectorState.STOPPED);
    private final AtomicReference<ConnectorState> updateResumeState = new AtomicReference<>(null);

    private volatile String name;
    private volatile String description;
    private volatile ConnectorConfiguration configuration;
    private volatile boolean performValidation = true;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    // Pending futures for state transitions; guarded by read/write lock
    private final List<CompletableFuture<Void>> pendingStartFutures = new ArrayList<>();
    private final List<CompletableFuture<Void>> pendingStopFutures = new ArrayList<>();


    public StandardConnectorNode(final String identifier, final ExtensionManager extensionManager, final Authorizable parentAuthorizable, final ProcessGroup managedProcessGroup,
        final ConnectorDetails connectorDetails, final String componentType, final BundleCoordinate bundleCoordinate) {

        this.identifier = identifier;
        this.extensionManager = extensionManager;
        this.parentAuthorizable = parentAuthorizable;
        this.managedProcessGroup = managedProcessGroup;
        this.connectorDetails = connectorDetails;
        this.componentType = componentType;
        this.bundleCoordinate = bundleCoordinate;
        this.configurationContext = createConfigurationContext();
    }

    private ConnectorConfigurationContext createConfigurationContext() {
        return new ConnectorConfigurationContext() {
            @Override
            public String getProperty(final String stepName, final String propertyName) {
                if (configuration == null) {
                    return null;
                }

                for (final ConfigurationStepConfiguration configurationStepConfiguration : configuration.getConfigurationStepConfigurations()) {
                    if (configurationStepConfiguration.getConfigurationStepName().equals(stepName)) {
                        for (final PropertyGroupConfiguration propertyGroupConfiguration : configurationStepConfiguration.getPropertyGroupConfigurations()) {
                            final Map<String, String> propertyValues = propertyGroupConfiguration.getPropertyValues();
                            if (propertyValues.containsKey(propertyName)) {
                                return propertyValues.get(propertyName);
                            }
                        }
                    }
                }

                return null;
            }

            @Override
            public String getProperty(final ConfigurationStep configurationStep, final ConnectorPropertyDescriptor connectorPropertyDescriptor) {
                return getProperty(configurationStep.getName(), connectorPropertyDescriptor.getName());
            }
        };
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
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    public ConnectorConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void prepareUpdate(final ScheduledExecutorService scheduler) throws FlowUpdateException {
        final ConnectorState initialState = getCurrentState();
        if (initialState != ConnectorState.RUNNING && initialState != ConnectorState.STOPPED && initialState != ConnectorState.DISABLED) {
            throw new IllegalStateException("Cannot prepare " + this + " for update because its state is currently " + initialState
                                            + "; it must be either RUNNING, STOPPED, or DISABLED.");
        }

        updateResumeState.set(initialState);
        setDesiredState(ConnectorState.READY_FOR_UPDATES);
        setCurrentState(ConnectorState.PREPARING_FOR_UPDATE);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().prepareUpdate();
            setCurrentState(ConnectorState.READY_FOR_UPDATES);
        } catch (final Throwable t) {
            logger.error("Failed to prepare update for {}", this, t);
            setCurrentState(ConnectorState.UPDATE_FAILED);

            try {
                getConnector().abortUpdatePreparation(t);
            } catch (final Throwable abortFailure) {
                logger.error("Failed to abort update preparation for {}", this, abortFailure);
            }

            throw t;
        }
    }

    @Override
    public void finishUpdate(final ScheduledExecutorService scheduler) throws FlowUpdateException {
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.UPDATING && currentState != ConnectorState.READY_FOR_UPDATES) {
            throw new IllegalStateException("Cannot finish update for " + this + " because its state is currently " + currentState
                                            + "; it must be PREPARING_FOR_UPDATE or UPDATING.");
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().finishUpdate();
        } catch (final Throwable t) {
            logger.error("Failed to finish update for {}", this, t);
            setCurrentState(ConnectorState.UPDATE_FAILED);
            setDesiredState(ConnectorState.UPDATE_FAILED);

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
    public void abortUpdatePreparation(final Throwable cause) {
        setCurrentState(ConnectorState.UPDATE_FAILED);
        setDesiredState(ConnectorState.UPDATE_FAILED);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().abortUpdatePreparation(cause);
        }
    }

    @Override
    public void setConfiguration(final ConnectorConfiguration configuration) throws FlowUpdateException {
        // Ensure that the Connector is fully stopped before allowing configuration to be updated
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.READY_FOR_UPDATES && currentState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot update the configuration of " + this + " because its state is currently " + currentState
                                            + "; it must be ready for updates before it can be configured.");
        }

        // Desired State must also be READY_FOR_UPDATES or UPDATING to ensure that the Connector is not transitioning to a new state during the configuration change
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.READY_FOR_UPDATES && desiredState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot update the configuration of " + this + " because its desired state is currently " + desiredState
                                            + "; it must be ready for updates before it can be configured.");
        }

        // Determine which configuration steps will change as a result of applying this new configuration
        final List<String> changedConfigurationSteps = determineChangedConfigurationSteps(this.configuration, configuration);

        this.configuration = configuration;

        final Connector connector = connectorDetails.getConnector();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connector.getClass(), getIdentifier())) {
            for (final String changedStep : changedConfigurationSteps) {
                logger.debug("Notifying {} of configuration change for configuration step {}", this, changedStep);
                connector.onConfigurationStepConfigured(changedStep);
            }
        } catch (final FlowUpdateException e) {
            throw e;
        } catch (final Exception e) {
            logger.error("Failed to invoke onConfigured for {}", this, e);
            throw new RuntimeException("Failed to invoke onConfigured for " + this, e);
        }
    }

    private List<String> determineChangedConfigurationSteps(final ConnectorConfiguration oldConfig, final ConnectorConfiguration newConfig) {
        final List<String> changedConfigurationSteps = new ArrayList<>();

        if (oldConfig == null) {
            // If there was no previous configuration, all configuration steps are considered changed
            for (final ConfigurationStepConfiguration configurationStepConfiguration : newConfig.getConfigurationStepConfigurations()) {
                changedConfigurationSteps.add(configurationStepConfiguration.getConfigurationStepName());
            }

            return changedConfigurationSteps;
        }

        final Map<String, List<PropertyGroupConfiguration>> oldPropertyGroupsByConfigurationStep = mapConfigurationSteps(oldConfig);
        final Map<String, List<PropertyGroupConfiguration>> newPropertyGroupsByConfigurationStep = mapConfigurationSteps(newConfig);

        // Check for changes in existing configuration steps and removed configuration steps
        for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : oldPropertyGroupsByConfigurationStep.entrySet()) {
            final String configurationStepName = entry.getKey();
            final List<PropertyGroupConfiguration> oldPropertyGroups = entry.getValue();
            final List<PropertyGroupConfiguration> newPropertyGroups = newPropertyGroupsByConfigurationStep.get(configurationStepName);
            if (newPropertyGroups == null) {
                // Entire configuration step has been removed
                changedConfigurationSteps.add(configurationStepName);
                continue;
            }

            final Map<String, Map<String, String>> oldPropertiesByPropertyGroup = new HashMap<>();
            for (final PropertyGroupConfiguration propertyGroupConfiguration : oldPropertyGroups) {
                oldPropertiesByPropertyGroup.putAll(mapPropertyGroups(propertyGroupConfiguration));
            }

            final Map<String, Map<String, String>> newPropertiesByPropertyGroup = new HashMap<>();
            for (final PropertyGroupConfiguration propertyGroupConfiguration : newPropertyGroups) {
                newPropertiesByPropertyGroup.putAll(mapPropertyGroups(propertyGroupConfiguration));
            }

            for (final Map.Entry<String, Map<String, String>> propertyGroupEntry : oldPropertiesByPropertyGroup.entrySet()) {
                final String propertyGroupName = propertyGroupEntry.getKey();
                final Map<String, String> oldProperties = propertyGroupEntry.getValue();
                final Map<String, String> newProperties = newPropertiesByPropertyGroup.get(propertyGroupName);
                if (newProperties == null) {
                    // Entire property group has been removed
                    changedConfigurationSteps.add(configurationStepName);
                    break;
                }

                for (final Map.Entry<String, String> propertyEntry : oldProperties.entrySet()) {
                    final String propertyName = propertyEntry.getKey();
                    final String oldValue = propertyEntry.getValue();
                    final String newValue = newProperties.get(propertyName);
                    if (!Objects.equals(oldValue, newValue)) {
                        changedConfigurationSteps.add(configurationStepName);
                        break;
                    }
                }
            }
        }

        // Check for newly added configuration steps
        for (final String newConfigurationStepName : newPropertyGroupsByConfigurationStep.keySet()) {
            if (!oldPropertyGroupsByConfigurationStep.containsKey(newConfigurationStepName)) {
                changedConfigurationSteps.add(newConfigurationStepName);
            }
        }

        return changedConfigurationSteps;
    }

    private Map<String, List<PropertyGroupConfiguration>> mapConfigurationSteps(final ConnectorConfiguration config) {
        final Map<String, List<PropertyGroupConfiguration>> configurationSteps = new HashMap<>();
        for (final ConfigurationStepConfiguration configurationStepConfiguration : config.getConfigurationStepConfigurations()) {
            configurationSteps.put(configurationStepConfiguration.getConfigurationStepName(), configurationStepConfiguration.getPropertyGroupConfigurations());
        }

        return configurationSteps;
    }

    private Map<String, Map<String, String>> mapPropertyGroups(final PropertyGroupConfiguration propertyGroupConfiguration) {
        final Map<String, Map<String, String>> propertyMap = new HashMap<>();
        propertyMap.put(propertyGroupConfiguration.getPropertyGroupName(), propertyGroupConfiguration.getPropertyValues());
        return propertyMap;
    }

    @Override
    public ConnectorState getCurrentState() {
        return currentState.get();
    }

    @Override
    public ConnectorState getDesiredState() {
        return desiredState.get();
    }

    @Override
    public void enable() {
        setDesiredState(ConnectorState.STOPPED);
        if (trySetCurrentState(ConnectorState.DISABLED, ConnectorState.STOPPED)) {
            logger.info("Transitioned current state for {} to {}", this, ConnectorState.STOPPED);
            return;
        }

        logger.info("{} enabled but not currently DISABLED so set desired state to STOPPED; current state is {}", this, currentState.get());
    }

    @Override
    public void disable() {
        setDesiredState(ConnectorState.DISABLED);

        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.DISABLED || currentState == ConnectorState.STOPPED || currentState == ConnectorState.UPDATE_FAILED) {
            if (trySetCurrentState(currentState, ConnectorState.DISABLED)) {
                logger.info("Transitioned current state for {} to {}", this, ConnectorState.DISABLED);
                return;
            }
        }

        logger.info("{} disabled but not in a state that can immediately transition to DISABLED so set desired state to DISABLED; current state is {}", this, currentState);
    }

    private void setDesiredState(final ConnectorState desiredState) {
        this.desiredState.set(desiredState);
        logger.info("Desired State for {} set to {}", this, desiredState);
    }

    private boolean trySetCurrentState(final ConnectorState expected, final ConnectorState newState) {
        final boolean changed = currentState.compareAndSet(expected, newState);
        if (changed) {
            logger.info("Transitioned current state for {} from {} to {}", this, expected, newState);
            // Complete appropriate futures when state is successfully updated
            completeFuturesForStateTransition(newState);
        }

        return changed;
    }

    private void setCurrentState(final ConnectorState newState) {
        final ConnectorState oldState = currentState.getAndSet(newState);
        logger.info("Transitioned current state for {} from {} to {}", this, oldState, newState);

        // Complete appropriate futures when state changes
        completeFuturesForStateTransition(newState);
    }

    private void completeFuturesForStateTransition(final ConnectorState newState) {
        // Complete start futures when transitioning to RUNNING
        if (newState == ConnectorState.RUNNING) {
            writeLock.lock();
            try {
                final List<CompletableFuture<Void>> futuresToComplete = new ArrayList<>(pendingStartFutures);
                pendingStartFutures.clear();

                for (final CompletableFuture<Void> future : futuresToComplete) {
                    future.complete(null);
                }

                if (!futuresToComplete.isEmpty()) {
                    logger.debug("Completed {} pending start futures for {}", futuresToComplete.size(), this);
                }
            } finally {
                writeLock.unlock();
            }
        }

        // Complete stop futures when transitioning to STOPPED or DISABLED
        if (newState == ConnectorState.STOPPED) {
            writeLock.lock();
            try {
                final List<CompletableFuture<Void>> futuresToComplete = new ArrayList<>(pendingStopFutures);
                pendingStopFutures.clear();

                for (final CompletableFuture<Void> future : futuresToComplete) {
                    future.complete(null);
                }

                if (!futuresToComplete.isEmpty()) {
                    logger.debug("Completed {} pending stop futures for {}", futuresToComplete.size(), this);
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public Future<Void> start(final ScheduledExecutorService scheduler) {
        final CompletableFuture<Void> startCompleteFuture = new CompletableFuture<>();
        start(scheduler, startCompleteFuture);
        return startCompleteFuture;
    }

    private void start(final ScheduledExecutorService scheduler, final CompletableFuture<Void> startCompleteFuture) {
        verifyCanStart();

        // Ensure that we're in the proper state to start and update the desired and current states
        writeLock.lock();
        try {
            setDesiredState(ConnectorState.RUNNING);
            final ConnectorState currentState = getCurrentState();

            switch (currentState) {
                case STARTING -> {
                    logger.debug("{} is already starting; adding future to pending start futures", this);
                }
                case RUNNING -> {
                    logger.debug("{} is already {}; will not attempt to start", this, currentState);
                    startCompleteFuture.complete(null);
                }
                case STOPPING -> {
                    // We have set the Desired State to RUNNING so when the Connector fully stops, it will be started again automatically
                    logger.info("{} is currently stopping so will not trigger Connector to start until it has fully stopped", this);
                }
                case STOPPED, PREPARING_FOR_UPDATE -> {
                    setCurrentState(ConnectorState.STARTING);
                    scheduler.schedule(() -> startComponent(scheduler, startCompleteFuture), 0, TimeUnit.SECONDS);
                }
            }
        } finally {
            if (!startCompleteFuture.isDone()) {
                // If we didn't complete the future above, we must have added it to pendingStartFutures
                pendingStartFutures.add(startCompleteFuture);
            }

            writeLock.unlock();
        }
    }

    @Override
    public Future<Void> stop(final ScheduledExecutorService scheduler) {
        final CompletableFuture<Void> stopCompleteFuture = new CompletableFuture<>();

        // Ensure that we're in the proper state to stop and update the desired and current states
        writeLock.lock();
        try {
            setDesiredState(ConnectorState.STOPPED);

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
                    pendingStopFutures.add(stopCompleteFuture);
                    return stopCompleteFuture;
                }

                stateUpdated = trySetCurrentState(currentState, ConnectorState.STOPPING);
            }
        } finally {
            writeLock.unlock();
        }

        scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 0, TimeUnit.SECONDS);

        return stopCompleteFuture;
    }

    private void stopComponent(final ScheduledExecutorService scheduler, final CompletableFuture<Void> stopCompleteFuture) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            connectorDetails.getConnector().stop();
        } catch (final Exception e) {
            logger.error("Failed to stop {}. Will try again in 10 seconds", this, e);
            scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 10, TimeUnit.SECONDS);
            return;
        }

        setCurrentState(ConnectorState.STOPPED);
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

        setCurrentState(ConnectorState.RUNNING);
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
            final List<ValidationResult> results = getConnector().validate();
            if (results == null || results.isEmpty()) {
                return new ValidationState(ValidationStatus.VALID, Collections.emptyList());
            }

            final boolean allValid = results.stream().allMatch(ValidationResult::isValid);
            final ValidationStatus status = allValid ? ValidationStatus.VALID : ValidationStatus.INVALID;
            return new ValidationState(status, results);
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
        return "StandardConnectorNode[id=" + identifier + ", name=" + name + ", state=" + currentState.get() + "]";
    }
}
