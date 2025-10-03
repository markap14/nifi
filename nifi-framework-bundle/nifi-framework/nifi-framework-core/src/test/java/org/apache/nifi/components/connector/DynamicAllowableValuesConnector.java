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
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.ConnectorUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DynamicAllowableValuesConnector extends AbstractConnector {
    static final String COLORS_STEP_NAME = "Colors";

    static final ConnectorPropertyDescriptor FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name("File Path")
        .description("The path to the file")
        .required(true)
        .build();

    static final ConnectorPropertyGroup FILE_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("")
        .addProperty(FILE_PATH)
        .build();

    static final ConfigurationStep FILE_STEP = new ConfigurationStep.Builder()
        .name("File")
        .subGroups(List.of(FILE_PROPERTY_GROUP))
        .build();


    @Override
    public List<String> getConfigurationStepNames() {
        final List<String> stepNames = new ArrayList<>();
        stepNames.add(FILE_STEP.getName());
        if (getProperty(FILE_STEP, FILE_PATH) != null) {
            stepNames.add(COLORS_STEP_NAME);
        }
        return stepNames;
    }

    @Override
    public ConfigurationStep getConfigurationStep(final String stepName) {
        if (stepName.equals(COLORS_STEP_NAME)) {
            final Set<ProcessorFacade> processorsFacades = getInitializationContext().getRootGroup().getProcessors();
            if (processorsFacades.isEmpty()) {
                return null;
            }

            final ProcessorFacade processorFacade = processorsFacades.iterator().next();
            try {
                final List<String> fileValues = (List<String>) processorFacade.invokeConnectorMethod("getFileValues", Map.of());
                return createColorConfigurationStep(fileValues);
            } catch (final InvocationFailedException e) {
                throw new RuntimeException(e);
            }
        } else if (stepName.equals(FILE_STEP.getName())) {
            return FILE_STEP;
        }

        return null;
    }

    private ConfigurationStep createColorConfigurationStep(final List<String> values) {
        final ConnectorPropertyDescriptor FIRST_PRIMARY_COLOR = new ConnectorPropertyDescriptor.Builder()
            .name("First Primary Color")
            .description("The first primary color")
            .defaultValue(values.getFirst())
            .allowableValues(values.toArray(new String[0]))
            .required(true)
            .build();

        final ConnectorPropertyGroup PRIMARY_COLORS_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Primary Colors")
            .addProperty(FIRST_PRIMARY_COLOR)
            .build();

        return new ConfigurationStep.Builder()
            .name("Colors")
            .subGroups(List.of(PRIMARY_COLORS_PROPERTY_GROUP))
            .build();
    }

    @Override
    public void onConfigured() throws FlowUpdateException {
        final VersionedExternalFlow externalFlow = ConnectorUtils.loadFlowFromResource("flows/choose-color.json");
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();
        final VersionedProcessor processor = rootGroup.getProcessors().iterator().next();
        processor.setProperties(Map.of("File", getProperty(FILE_STEP, FILE_PATH)));

        getInitializationContext().updateFlow(externalFlow);
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName) {
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
        return List.of();
    }
}
