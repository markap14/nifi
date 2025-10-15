/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.controller.flow;

import org.apache.nifi.components.connector.facades.standalone.ComponentContextProvider;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.StandardProcessContext;

import java.util.Map;

public class StandardComponentContextProvider implements ComponentContextProvider {
    private final FlowController flowController;

    public StandardComponentContextProvider(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public ProcessContext createProcessContext(final ProcessorNode processorNode) {
        final StateManager stateManager = flowController.getStateManagerProvider().getStateManager(processorNode.getIdentifier());
        final TaskTermination taskTermination = () -> false;
        return new StandardProcessContext(processorNode, flowController.getControllerServiceProvider(), stateManager, taskTermination, flowController);
    }

    @Override
    public ProcessContext createProcessContext(final ProcessorNode processorNode, final Map<String, String> propertiesOverride, final ParameterLookup parameterLookup) {
        final StateManager stateManager = flowController.getStateManagerProvider().getStateManager(processorNode.getIdentifier());
        final TaskTermination taskTermination = () -> false;
        return new StandardProcessContext(processorNode, propertiesOverride, null, parameterLookup, flowController.getControllerServiceProvider(), stateManager, taskTermination, flowController);
    }

    @Override
    public ConfigurationContext createConfigurationContext(final ControllerServiceNode serviceNode) {
        return new StandardConfigurationContext(serviceNode, flowController.getControllerServiceProvider(), null);
    }

    @Override
    public ConfigurationContext createConfigurationContext(final ControllerServiceNode serviceNode, final Map<String, String> propertiesOverride, final ParameterLookup parameterLookup) {
        return new StandardConfigurationContext(serviceNode, propertiesOverride, null, parameterLookup, flowController.getControllerServiceProvider(), null);
    }
}
