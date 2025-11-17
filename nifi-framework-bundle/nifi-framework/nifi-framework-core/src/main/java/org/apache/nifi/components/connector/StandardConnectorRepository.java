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

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class StandardConnectorRepository implements ConnectorRepository {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorRepository.class);

    private final Map<String, ConnectorNode> connectors = new HashMap<>();
    private final FlowEngine lifecycleExecutor = new FlowEngine(8, "NiFi Connector Lifecycle");

    private volatile ExtensionManager extensionManager;
    private volatile ConnectorRequestReplicator requestReplicator;

    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
        this.extensionManager = context.getExtensionManager();
        this.requestReplicator = context.getRequestReplicator();
    }

    @Override
    public synchronized void addConnector(final ConnectorNode connector) {
        connectors.put(connector.getIdentifier(), connector);
    }

    @Override
    public void restoreConnector(final ConnectorNode connector) {
        addConnector(connector);
    }

    @Override
    public void removeConnector(final String connectorId) {
        final ConnectorNode connectorNode = connectors.get(connectorId);
        if (connectorNode == null) {
            throw new IllegalStateException("No connector found with ID " + connectorId);
        }

        connectorNode.verifyCanDelete();
        connectors.remove(connectorId);

        final Class<?> taskClass = connectorNode.getConnector().getClass();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, taskClass, connectorId)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, connectorNode.getConnector());
        }

        extensionManager.removeInstanceClassLoader(connectorId);
    }

    @Override
    public ConnectorNode getConnector(final String identifier) {
        return connectors.get(identifier);
    }

    @Override
    public List<ConnectorNode> getConnectors() {
        return List.copyOf(connectors.values());
    }

    @Override
    public Future<Void> startConnector(final ConnectorNode connector) {
        return connector.start(lifecycleExecutor);
    }

    @Override
    public Future<Void> stopConnector(final ConnectorNode connector) {
        return connector.stop(lifecycleExecutor);
    }

    @Override
    public void applyUpdate(final ConnectorNode connector) throws FlowUpdateException {
        connector.prepareForUpdate(lifecycleExecutor);

        try {
            // Wait for Connector State to become UPDATING
            while (true) {
                final ConnectorState clusterState = requestReplicator.getState(connector.getIdentifier());
                if (clusterState == ConnectorState.UPDATE_FAILED) {
                    throw new FlowUpdateException("State of " + connector + " transitioned to UPDATE_FAILED");
                } else if (clusterState == ConnectorState.UPDATING) {
                    logger.info("State for {} is now UPDATING; will apply update", connector);
                    break;
                } else if (clusterState == ConnectorState.PREPARING_FOR_UPDATE) {
                    logger.debug("State for {} is still PREPARING_FOR_UPDATE", connector);
                    Thread.sleep(Duration.ofSeconds(1));
                    continue;
                }

                throw new FlowUpdateException("State of " + connector + " transitioned to unexpected state: " + clusterState);
            }

            // Apply the update to the connector.
            connector.applyUpdate(lifecycleExecutor);
        } catch (final Exception e) {
            connector.abortUpdate(e);
        }
    }

    @Override
    public void configureConnector(final ConnectorNode connector, final String stepName, final List<PropertyGroupConfiguration> stepConfiguration) throws FlowUpdateException {
        connector.setConfiguration(stepName, stepConfiguration);
    }

    @Override
    public void inheritConfiguration(final ConnectorNode connector, final List<VersionedConfigurationStep> flowConfiguration) throws FlowUpdateException {
        connector.prepareForUpdate(lifecycleExecutor);

        try {
            connector.inheritConfiguration(flowConfiguration);
        } catch (final Exception e) {
            connector.abortUpdate(e);
            throw e;
        }
    }

    @Override
    public ConnectorStateTransition createStateTransition(final String type, final String id) {
        final String componentDescription = "StandardConnectorNode[id=" + id + ", type=" + type + "]";
        return new StandardConnectorStateTransition(componentDescription);
    }

    @Override
    public ConnectorInitializationContextBuilder createInitializationContextBuilder() {
        return new StandardConnectorInitializationContext.Builder();
    }

}