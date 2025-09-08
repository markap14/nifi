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
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private final AtomicReference<ScheduledState> currentState = new AtomicReference<>(ScheduledState.STOPPED);
    private final AtomicReference<ScheduledState> desiredState = new AtomicReference<>(ScheduledState.STOPPED);

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
    public void setConfiguration(final ConnectorConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public ScheduledState getCurrentState() {
        return currentState.get();
    }

    @Override
    public ScheduledState getDesiredState() {
        return desiredState.get();
    }

    @Override
    public void enable() {
        setDesiredState(ScheduledState.STOPPED);
        if (trySetCurrentState(ScheduledState.DISABLED, ScheduledState.STOPPED)) {
            logger.info("Transitioned current state for {} to {}", this, ScheduledState.STOPPED);
            return;
        }

        logger.info("{} enabled but not currently DISABLED so set desired state to STOPPED; current state is {}", this, currentState.get());
    }

    @Override
    public void disable() {
        setDesiredState(ScheduledState.DISABLED);
        if (trySetCurrentState(ScheduledState.STOPPED, ScheduledState.DISABLED)) {
            logger.info("Transitioned current state for {} to {}", this, ScheduledState.DISABLED);
            return;
        }

        logger.info("{} disabled but not currently STOPPED so set desired state to DISABLED; current state is {}", this, currentState.get());
    }

    private void setDesiredState(final ScheduledState desiredState) {
        if (desiredState == ScheduledState.RUN_ONCE) {
            throw new IllegalArgumentException("Connectors cannot be scheduled to Run Once");
        }

        this.desiredState.set(desiredState);
        logger.info("Desired State for {} set to {}", this, desiredState);
    }

    private boolean trySetCurrentState(final ScheduledState expected, final ScheduledState newState) {
        if (newState == ScheduledState.RUN_ONCE) {
            throw new IllegalArgumentException("Connectors cannot be scheduled to Run Once");
        }

        final boolean changed = currentState.compareAndSet(expected, newState);
        if (changed) {
            logger.info("Transitioned current state for {} from {} to {}", this, expected, newState);
            // Complete appropriate futures when state is successfully updated
            completeFuturesForStateTransition(newState);
        }

        return changed;
    }

    private void setCurrentState(final ScheduledState newState) {
        final ScheduledState oldState = currentState.getAndSet(newState);
        logger.info("Transitioned current state for {} from {} to {}", this, oldState, newState);

        // Complete appropriate futures when state changes
        completeFuturesForStateTransition(newState);
    }

    private void completeFuturesForStateTransition(final ScheduledState newState) {
        // Complete start futures when transitioning to RUNNING
        if (newState == ScheduledState.RUNNING) {
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
        if (newState == ScheduledState.STOPPED) {
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
            setDesiredState(ScheduledState.RUNNING);

            boolean stateUpdated = false;
            while (!stateUpdated) {
                final ScheduledState currentState = getCurrentState();

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
                        stateUpdated = trySetCurrentState(currentState, ScheduledState.STARTING);
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
            setDesiredState(ScheduledState.STOPPED);

            boolean stateUpdated = false;
            while (!stateUpdated) {
                final ScheduledState currentState = getCurrentState();
                if (currentState == ScheduledState.STOPPED || currentState == ScheduledState.DISABLED) {
                    logger.debug("{} is already {}; will not attempt to stop", this, currentState);
                    stopCompleteFuture.complete(null);
                    return stopCompleteFuture;
                }
                
                if (currentState == ScheduledState.STOPPING) {
                    logger.debug("{} is already stopping; adding future to pending stop futures", this);
                    pendingStopFutures.add(stopCompleteFuture);
                    return stopCompleteFuture;
                }

                stateUpdated = trySetCurrentState(currentState, ScheduledState.STOPPING);
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

        setCurrentState(ScheduledState.STOPPED);
        stopCompleteFuture.complete(null);

        final ScheduledState desiredState = getDesiredState();
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
        final ScheduledState desiredState = getDesiredState();
        if (desiredState != ScheduledState.RUNNING) {
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

        setCurrentState(ScheduledState.RUNNING);
        startCompleteFuture.complete(null);
    }


    @Override
    public void verifyCanDelete() {
        final ScheduledState currentState = getCurrentState();
        if (currentState == ScheduledState.STOPPED || currentState == ScheduledState.DISABLED) {
            return;
        }

        throw new IllegalStateException("Cannot delete " + this + " because its state is currently " + currentState + "; it must be stopped before it can be deleted.");
    }

    @Override
    public void verifyCanStart() {
        final ScheduledState currentState = getCurrentState();
        if (currentState == ScheduledState.DISABLED) {
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
