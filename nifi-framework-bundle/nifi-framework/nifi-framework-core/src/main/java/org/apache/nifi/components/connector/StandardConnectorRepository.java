/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class StandardConnectorRepository implements ConnectorRepository {

    private final Map<String, ConnectorNode> connectors = new HashMap<>();
    private final ScheduledExecutorService lifecycleExecutor = Executors.newScheduledThreadPool(8, new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("NiFi Connector Lifecycle Thread-" + threadNumber.getAndIncrement());
            thread.start();
            return thread;
        }
    });

    private volatile ExtensionManager extensionManager;


    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
        this.extensionManager = context.getExtensionManager();
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
    public void prepareForUpdate(final ConnectorNode connector) throws FlowUpdateException {
        connector.prepareForUpdate(lifecycleExecutor);
    }

    @Override
    public void abortUpdatePreparation(final ConnectorNode connector, final Throwable cause) {
        connector.abortUpdate(cause);
    }

    @Override
    public void finishUpdate(final ConnectorNode connector) throws FlowUpdateException {
        connector.finishUpdate(lifecycleExecutor);
    }

    @Override
    public void configureConnector(final ConnectorNode connector, final String stepName, final List<PropertyGroupConfiguration> stepConfiguration) throws FlowUpdateException {
        connector.setConfiguration(stepName, stepConfiguration);
    }
}
