/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardConnectorRepository implements ConnectorRepository {

    private final Map<String, ConnectorNode> connectors = new HashMap<>();

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
}
