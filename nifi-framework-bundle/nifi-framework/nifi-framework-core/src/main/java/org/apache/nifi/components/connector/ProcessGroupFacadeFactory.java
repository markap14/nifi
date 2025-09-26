/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.groups.ProcessGroup;

public interface ProcessGroupFacadeFactory {
    ProcessGroupFacade create(ProcessGroup processGroup);
}
