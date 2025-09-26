/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.util.ConnectorUtils;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;

public class ParameterConnector extends AbstractConnector {
    private volatile boolean initialized = false;

    static final ConnectorPropertyDescriptor TEXT_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name("Text")
        .description("The text to write to FlowFiles")
        .type(PropertyType.STRING)
        .addValidator(Validator.VALID)
        .required(true)
        .defaultValue("Hello World")
        .build();

    private static final ConnectorPropertyGroup TEXT_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Text Configuration")
        .description("Configure the text to be written to FlowFiles")
        .subGroups(List.of(new ConnectorPropertySubGroup.Builder()
            .addProperty(TEXT_PROPERTY)
            .build()))
        .build();

    @Override
    protected void init() throws FlowUpdateException {
        // Load the base flow from the generate-and-log-with-parameter.json flow
        final VersionedExternalFlow externalFlow = ConnectorUtils.loadFlowFromResource("flows/generate-and-log-with-parameter.json");
        getInitializationContext().updateFlow(externalFlow, this::drainFlowFiles);
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public List<String> getPropertyGroupNames() {
        return List.of(TEXT_GROUP.getName());
    }

    @Override
    public ConnectorPropertyGroup getPropertyGroup(final String groupName) {
        if (TEXT_GROUP.getName().equals(groupName)) {
            return TEXT_GROUP;
        }
        return null;
    }

    @Override
    public void onConfigured() {
        try {
            updateTextParameter();
        } catch (final FlowUpdateException e) {
            getLogger().error("Failed to update Text parameter", e);
            throw new RuntimeException("Failed to update Text parameter", e);
        }
    }

    @Override
    public void onPropertyGroupConfigured(final String groupName) {
    }

    @Override
    public List<ValidationResult> validatePropertyGroup(final String groupName, final Map<String, String> propertyValues) {
        return List.of();
    }

    private void updateTextParameter() throws FlowUpdateException {
        final String textValue = getProperty(TEXT_GROUP, TEXT_PROPERTY);

        // Update the "Text" parameter with the configured property value
        final ParameterValue textParameter = new ParameterValue.Builder()
            .name("Text")
            .value(textValue)
            .sensitive(false)
            .build();

        getInitializationContext().getParameterContext().updateParameters(List.of(textParameter));
    }
}
