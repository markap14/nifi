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
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private final ProcessGroup managedProcessGroup;
    private final ConnectorDetails connectorDetails;
    private final String componentType;
    private final BundleCoordinate bundleCoordinate;
    private final ConnectorConfigurationContext configurationContext;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final AtomicReference<ConnectorState> currentState = new AtomicReference<>(ConnectorState.STOPPED);
    private final AtomicReference<ConnectorState> desiredState = new AtomicReference<>(ConnectorState.STOPPED);

    private volatile String name;
    private volatile String description;
    private volatile ConnectorConfiguration configuration;
    private volatile ProcessGroup parentProcessGroup;
    private volatile boolean performValidation = true;
    private volatile ConnectorParameterContext parameterContext;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    
    // Pending futures for state transitions; guarded by read/write lock
    private final List<CompletableFuture<Void>> pendingStartFutures = new ArrayList<>();
    private final List<CompletableFuture<Void>> pendingStopFutures = new ArrayList<>();


    public StandardConnectorNode(final String identifier, final ExtensionManager extensionManager, final ProcessGroup managedProcessGroup,
                final ConnectorDetails connectorDetails, final String componentType, final BundleCoordinate bundleCoordinate) {

        this.identifier = identifier;
        this.extensionManager = extensionManager;
        this.managedProcessGroup = managedProcessGroup;
        this.connectorDetails = connectorDetails;
        this.componentType = componentType;
        this.bundleCoordinate = bundleCoordinate;
        this.configurationContext = createConfigurationContext();
    }

    private ConnectorConfigurationContext createConfigurationContext() {
        return new ConnectorConfigurationContext() {
            @Override
            public String getProperty(final String groupName, final String propertyName) {
                if (configuration == null) {
                    final ConnectorPropertyDescriptor descriptor = getPropertyDescriptor(groupName, propertyName);
                    return descriptor == null ? null : descriptor.getDefaultValue();
                }

                for (final PropertyGroupConfiguration groupConfiguration : configuration.getPropertyGroupConfigurations()) {
                    if (groupConfiguration.getPropertyGroupName().equals(groupName)) {
                        for (final PropertySubGroupConfiguration subGroupConfig : groupConfiguration.getSubGroupConfigurations()) {
                            final Map<String, String> propertyValues = subGroupConfig.getPropertyValues();
                            if (propertyValues.containsKey(propertyName)) {
                                return propertyValues.get(propertyName);
                            }
                        }
                    }
                }

                return null;
            }

            @Override
            public String getProperty(final ConnectorPropertyGroup connectorPropertyGroup, final ConnectorPropertyDescriptor connectorPropertyDescriptor) {
                return getProperty(connectorPropertyGroup.getName(), connectorPropertyDescriptor.getName());
            }
        };
    }

    private ConnectorPropertyDescriptor getPropertyDescriptor(final String groupName, final String propertyName) {
        final ConnectorPropertyGroup propertyGroup = getConnector().getPropertyGroup(groupName);
        if (propertyGroup == null) {
            return null;
        }

        for (final ConnectorPropertySubGroup subgroup : propertyGroup.getSubGroups()) {
            for (final ConnectorPropertyDescriptor descriptor : subgroup.getProperties()) {
                if (descriptor.getName().equals(propertyName)) {
                    return descriptor;
                }
            }
        }

        return null;
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
    public void setConfiguration(final ConnectorConfiguration configuration) throws FlowUpdateException {
        // Ensure that the Connector is fully stopped before allowing configuration to be updated
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.STOPPED && currentState != ConnectorState.DISABLED) {
            throw new IllegalStateException("Cannot update the configuration of " + this + " because its state is currently " + currentState + "; it must be fully stopped before the configuration can be changed.");
        }

        // Desired State must also be STOPPED or DISABLED to ensure that the Connector is not transitioning to a new state during the configuration change
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.STOPPED && desiredState != ConnectorState.DISABLED) {
            throw new IllegalStateException("Cannot update the configuration of " + this + " because its desired state is currently " + desiredState + "; it must be fully stopped before the configuration can be changed.");
        }

        // Determine which property groups will change as a result of applying this new configuration
        final List<String> changedPropertyGroups = determineChangedPropertyGroups(this.configuration, configuration);

        this.configuration = configuration;

        final Connector connector = connectorDetails.getConnector();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connector.getClass(), getIdentifier())) {

            for (final String changedGroup : changedPropertyGroups) {
                logger.debug("Notifying {} of configuration change for property group {}", this, changedGroup);

                try {
                    connector.onPropertyGroupConfigured(changedGroup);
                } catch (final Throwable t) {
                    logger.error("{} Failed to notify Connector that property group {} was configured", this, changedGroup, t);
                }
            }

            connectorDetails.getConnector().onConfigured();
        } catch (final FlowUpdateException flowUpdateException) {
            throw flowUpdateException;
        } catch (final Exception e) {
            logger.error("Failed to invoke onConfigured for {}", this, e);
            throw new RuntimeException("Failed to invoke onConfigured for " + this, e);
        }
    }

    private List<String> determineChangedPropertyGroups(final ConnectorConfiguration oldConfig, final ConnectorConfiguration newConfig) {
        final List<String> changedPropertyGroups = new ArrayList<>();

        if (oldConfig == null) {
            // If there was no previous configuration, all property groups are considered changed
            for (final PropertyGroupConfiguration groupConfiguration : newConfig.getPropertyGroupConfigurations()) {
                changedPropertyGroups.add(groupConfiguration.getPropertyGroupName());
            }

            return changedPropertyGroups;
        }

        final Map<String, List<PropertySubGroupConfiguration>> oldSubGroupsByGroup = mapPropertyGroups(oldConfig);
        final Map<String, List<PropertySubGroupConfiguration>> newSubGroupsByGroup = mapPropertyGroups(newConfig);

        // Check for changes in existing groups and removed groups
        for (final Map.Entry<String, List<PropertySubGroupConfiguration>> entry : oldSubGroupsByGroup.entrySet()) {
            final String groupName = entry.getKey();
            final List<PropertySubGroupConfiguration> oldSubGroups = entry.getValue();
            final List<PropertySubGroupConfiguration> newSubGroups = newSubGroupsByGroup.get(groupName);
            if (newSubGroups == null) {
                // Entire group has been removed
                changedPropertyGroups.add(groupName);
                continue;
            }

            final Map<String, Map<String, String>> oldPropertiesBySubGroup = new HashMap<>();
            for (final PropertySubGroupConfiguration subGroupConfig : oldSubGroups) {
                oldPropertiesBySubGroup.putAll(mapPropertySubGroups(subGroupConfig));
            }

            final Map<String, Map<String, String>> newPropertiesBySubGroup = new HashMap<>();
            for (final PropertySubGroupConfiguration subGroupConfig : newSubGroups) {
                newPropertiesBySubGroup.putAll(mapPropertySubGroups(subGroupConfig));
            }

            for (final Map.Entry<String, Map<String, String>> subGroupEntry : oldPropertiesBySubGroup.entrySet()) {
                final String subGroupName = subGroupEntry.getKey();
                final Map<String, String> oldProperties = subGroupEntry.getValue();
                final Map<String, String> newProperties = newPropertiesBySubGroup.get(subGroupName);
                if (newProperties == null) {
                    // Entire sub-group has been removed
                    changedPropertyGroups.add(groupName);
                    break;
                }

                for (final Map.Entry<String, String> propertyEntry : oldProperties.entrySet()) {
                    final String propertyName = propertyEntry.getKey();
                    final String oldValue = propertyEntry.getValue();
                    final String newValue = newProperties.get(propertyName);
                    if (!Objects.equals(oldValue, newValue)) {
                        changedPropertyGroups.add(groupName);
                        break;
                    }
                }
            }
        }
        
        // Check for newly added groups
        for (final String newGroupName : newSubGroupsByGroup.keySet()) {
            if (!oldSubGroupsByGroup.containsKey(newGroupName)) {
                changedPropertyGroups.add(newGroupName);
            }
        }

        return changedPropertyGroups;
    }

    private Map<String, List<PropertySubGroupConfiguration>> mapPropertyGroups(final ConnectorConfiguration config) {
        final Map<String, List<PropertySubGroupConfiguration>> groups = new HashMap<>();
        for (final PropertyGroupConfiguration groupConfig : config.getPropertyGroupConfigurations()) {
            groups.put(groupConfig.getPropertyGroupName(), groupConfig.getSubGroupConfigurations());
        }

        return groups;
    }

    private Map<String, Map<String, String>> mapPropertySubGroups(final PropertySubGroupConfiguration subGroupConfiguration) {
        final Map<String, Map<String, String>> propertyMap = new HashMap<>();
        propertyMap.put(subGroupConfiguration.getSubGroupName(), subGroupConfiguration.getPropertyValues());
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
        if (trySetCurrentState(ConnectorState.STOPPED, ConnectorState.DISABLED)) {
            logger.info("Transitioned current state for {} to {}", this, ConnectorState.DISABLED);
            return;
        }

        logger.info("{} disabled but not currently STOPPED so set desired state to DISABLED; current state is {}", this, currentState.get());
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

            boolean stateUpdated = false;
            while (!stateUpdated) {
                final ConnectorState currentState = getCurrentState();

                switch (currentState) {
                    case STARTING -> {
                        logger.debug("{} is already starting; adding future to pending start futures", this);
                        return;
                    }
                    case RUNNING -> {
                        logger.debug("{} is already {}; will not attempt to start", this, currentState);
                        startCompleteFuture.complete(null);
                        return;
                    }
                    case STOPPING -> {
                        // We have set the Desired State to RUNNING so when the Connector fully stops, it will be started again automatically
                        logger.info("{} is currently stopping so will not trigger Connector to start until it has fully stopped", this);
                        return;
                    }
                    case STOPPED -> {
                        stateUpdated = trySetCurrentState(currentState, ConnectorState.STARTING);
                        scheduler.schedule(() -> startComponent(scheduler, startCompleteFuture), 0, TimeUnit.SECONDS);
                    }
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
    public void setParentProcessGroup(final ProcessGroup processGroup) {
        this.parentProcessGroup = processGroup;
    }

    @Override
    public ProcessGroup getParentProcessGroup() {
        return parentProcessGroup;
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
        return parentProcessGroup == null ? null : parentProcessGroup.getIdentifier();
    }

    @Override
    public ComponentLog getComponentLog() {
        return connectorDetails.getComponentLog();
    }

    @Override
    public ConnectorParameterContext getParameterContext() {
        return parameterContext;
    }

    @Override
    public void setParameterContext(final ConnectorParameterContext parameterContext) {
        this.parameterContext = parameterContext;
    }

    @Override
    public ConnectorConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return parentProcessGroup;
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
