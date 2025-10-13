/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.nar.ExtensionManager;

public interface ConnectorRepositoryInitializationContext {

    FlowManager getFlowManager();

    ExtensionManager getExtensionManager();

    NodeTypeProvider getNodeTypeProvider();

}
