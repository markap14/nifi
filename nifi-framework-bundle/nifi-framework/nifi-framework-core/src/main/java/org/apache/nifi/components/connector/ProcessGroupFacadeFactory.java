/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;

public interface ProcessGroupFacadeFactory {
    ProcessGroupFacade create(ProcessGroup processGroup, ComponentLog connectorLogger);
}
