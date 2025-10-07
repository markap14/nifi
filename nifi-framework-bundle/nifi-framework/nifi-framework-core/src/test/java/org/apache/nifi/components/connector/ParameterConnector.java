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
import org.apache.nifi.processor.util.StandardValidators;

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

    static final ConnectorPropertyDescriptor SLEEP_DURATION = new ConnectorPropertyDescriptor.Builder()
        .name("Sleep Duration")
        .description("The duration to sleep when the Sleep Processor is stopped")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(true)
        .defaultValue("1 sec")
        .build();

    private static final ConfigurationStep TEXT_STEP = new ConfigurationStep.Builder()
        .name("Text Configuration")
        .description("Configure the text to be written to FlowFiles")
        .propertyGroups(List.of(new ConnectorPropertyGroup.Builder()
            .addProperty(TEXT_PROPERTY)
            .addProperty(SLEEP_DURATION)
            .build()))
        .build();

    @Override
    protected void init() throws FlowUpdateException {
        // Load the base flow from the generate-and-log-with-parameter.json flow
        final VersionedExternalFlow externalFlow = ConnectorUtils.loadFlowFromResource("flows/generate-and-log-with-parameter.json");
        getInitializationContext().updateFlow(externalFlow);
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(TEXT_STEP);
    }

    @Override
    public void finishUpdate() {
        try {
            updateTextParameter();
        } catch (final FlowUpdateException e) {
            getLogger().error("Failed to update parameters", e);
            throw new RuntimeException("Failed to update parameters", e);
        }
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName) {
    }

    @Override
    public void prepareUpdate() {
    }

    @Override
    public void abortUpdatePreparation(final Throwable throwable) {
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
        return List.of();
    }

    private void updateTextParameter() throws FlowUpdateException {
        final String textValue = getProperty(TEXT_STEP, TEXT_PROPERTY);

        // Update the "Text" parameter with the configured property value
        final ParameterValue textParameter = new ParameterValue.Builder()
            .name("Text")
            .value(textValue)
            .sensitive(false)
            .build();

        final ParameterValue sleepDurationParameter = new ParameterValue.Builder()
            .name("Sleep Duration")
            .value(getProperty(TEXT_STEP, SLEEP_DURATION))
            .sensitive(false)
            .build();

        final List<ParameterValue> parameterValues = List.of(textParameter, sleepDurationParameter);
        getInitializationContext().getParameterContext().updateParameters(parameterValues);
    }
}
