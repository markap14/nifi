/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.logging.ComponentLog;

public class ConnectorDetails {
    private final Connector connector;
    private final ComponentLog componentLog;
    private final BundleCoordinate bundleCoordinate;

    public ConnectorDetails(final Connector connector, final BundleCoordinate bundleCoordinate, final ComponentLog logger) {
        this.connector = connector;
        this.bundleCoordinate = bundleCoordinate;
        this.componentLog = logger;
    }

    public Connector getConnector() {
        return connector;
    }

    public ComponentLog getComponentLog() {
        return componentLog;
    }

    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }
}
