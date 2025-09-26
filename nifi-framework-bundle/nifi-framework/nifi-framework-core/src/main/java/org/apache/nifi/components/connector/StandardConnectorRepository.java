/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

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

    @Override
    public void initialize(final ConnectorRepositoryInitializationContext context) {
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
    public void removeConnector(final ConnectorNode connector) {
        connectors.remove(connector.getIdentifier());
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
}
