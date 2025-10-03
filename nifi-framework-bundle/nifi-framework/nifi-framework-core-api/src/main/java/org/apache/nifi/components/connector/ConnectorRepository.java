/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import java.util.List;
import java.util.concurrent.Future;

public interface ConnectorRepository {

    void initialize(ConnectorRepositoryInitializationContext context);

    /**
     * Adds the given Connector to the Repository
     * @param connector the Connector to add
     */
    void addConnector(ConnectorNode connector);

    /**
     * Restores a previously added Connector to the Repository on restart.
     * This is differentiated from addConnector in that this method is not called
     * for newly created Connectors during the typical lifecycle of NiFi, but rather
     * only to notify the Repository of Connectors that were present when NiFi was last shutdown.
     *
     * @param connector the Connector to restore
     */
    void restoreConnector(ConnectorNode connector);

    /**
     * Removes the given Connector from the Repository
     * @param connector the Connector to remove
     */
    void removeConnector(ConnectorNode connector);

    /**
     * Gets the Connector with the given identifier
     * @param identifier the identifier of the Connector to get
     * @return the Connector with the given identifier, or null if no such Connector exists
     */
    ConnectorNode getConnector(String identifier);

    /**
     * @return all Connectors in the Repository
     */
    List<ConnectorNode> getConnectors();

    /**
     * Starts the given Connector, managing any appropriate lifecycle events.
     * @param connector the Connector to start
     * @return a CompletableFuture that will be completed when the Connector has started
     */
    Future<Void> startConnector(ConnectorNode connector);

    /**
     * Stops the given Connector, managing any appropriate lifecycle events.
     * @param connector the Connector to stop
     * @return a CompletableFuture that will be completed when the Connector has stopped
     */
    Future<Void> stopConnector(ConnectorNode connector);

    void configureConnector(ConnectorNode connector, ConnectorConfiguration configuration) throws FlowUpdateException;
}
