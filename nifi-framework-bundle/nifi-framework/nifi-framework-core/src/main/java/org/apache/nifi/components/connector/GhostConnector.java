/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ValidationResult;
import org.eclipse.tags.shaded.org.apache.bcel.verifier.VerificationResult;

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
    public String toString() {
        return "GhostConnector[id=" + identifier + ", type=" + canonicalClassName + "]";
    }
}
