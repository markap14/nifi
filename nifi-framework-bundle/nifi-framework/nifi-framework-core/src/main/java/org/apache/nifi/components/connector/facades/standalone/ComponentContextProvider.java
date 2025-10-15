/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;

import java.util.Map;

public interface ComponentContextProvider {
    ProcessContext createProcessContext(ProcessorNode processorNode);

    ProcessContext createProcessContext(ProcessorNode processorNode, Map<String, String> propertiesOverride, ParameterLookup parameterLookup);

    ConfigurationContext createConfigurationContext(ControllerServiceNode serviceNode);

    ConfigurationContext createConfigurationContext(ControllerServiceNode serviceNode, Map<String, String> propertiesOverride, ParameterLookup parameterLookup);

}
