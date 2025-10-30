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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ValidationResult;

import java.util.List;
import java.util.Map;

public class GhostConnector implements Connector {
    private final String identifier;
    private final String canonicalClassName;
    private final List<ValidationResult> validationResults;
    private final List<ConfigVerificationResult> configVerificationResults;

    public GhostConnector(final String identifier, final String canonicalClassName) {
        this.identifier = identifier;
        this.canonicalClassName = canonicalClassName;

        validationResults = List.of(new ValidationResult.Builder()
            .subject("Missing Connector")
            .valid(false)
            .explanation("Could not create Connector of type " + canonicalClassName)
            .build());

        configVerificationResults = List.of(new ConfigVerificationResult.Builder()
            .verificationStepName("Create Connector")
            .outcome(Outcome.FAILED)
            .explanation("Could not create Connector of type " + canonicalClassName)
            .build());
    }

    @Override
    public void initialize(final ConnectorInitializationContext connectorInitializationContext) {
    }

    @Override
    public void start() throws FlowUpdateException {
    }

    @Override
    public void stop() throws FlowUpdateException {
    }

    @Override
    public List<ValidationResult> validate() {
        return List.of(new ValidationResult.Builder()
            .subject("Missing Connector")
            .input("Any Property")
            .valid(false)
            .explanation("Connector is of type " + canonicalClassName + ", but this Connector implementation could not be created")
            .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName) {
    }

    @Override
    public void prepareForUpdate() {
    }

    @Override
    public void abortUpdatePreparation(final Throwable throwable) {
    }

    @Override
    public void finishUpdate() {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
        return configVerificationResults;
    }

    @Override
    public List<ValidationResult> validate(final ConnectorConfigurationContext connectorConfigurationContext) {
        return validationResults;
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName, final String filter) {
        return List.of();
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName) {
        return List.of();
    }

    @Override
    public String toString() {
        return "GhostConnector[id=" + identifier + ", type=" + canonicalClassName + "]";
    }
}
