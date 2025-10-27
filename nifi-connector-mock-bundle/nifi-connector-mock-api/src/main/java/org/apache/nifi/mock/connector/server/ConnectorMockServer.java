/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.NiFiServer;

public interface ConnectorMockServer extends NiFiServer, ConnectorTestRunner {

    void instantiateConnector(String connectorClassName);

}
