/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

public enum ConnectorState {
    STARTING,
    RUNNING,
    STOPPING,
    STOPPED,
    DISABLED,
    PREPARING_FOR_UPDATE,
    READY_FOR_UPDATES,
    UPDATING,
    UPDATE_FAILED;
}
