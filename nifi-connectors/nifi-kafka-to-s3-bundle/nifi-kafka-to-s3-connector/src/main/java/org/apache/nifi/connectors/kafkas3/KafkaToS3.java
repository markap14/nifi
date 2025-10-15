/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: S3 Config
// TODO: Implement Connector Method in ConsumeKafka to get sample data from a topic
// TODO: Implement Connector Method in Reader to parse sample data
// TODO: Rename ConnectorUtils to VersionedFlowUtils
// TODO: Validate
//       - Can Connect to Kafka
//       - Topics Exists
//       - Can Connect to Schema Registry (if configured)
//       - Sample Data can be parsed by selected format
//       - Permissions to write to S3 bucket
public class KafkaToS3 extends AbstractConnector {


    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(
            KafkaConnectionStep.KAFKA_CONNECTION_STEP,
            KafkaTopicsStep.createConfigurationStep(getAvailableTopics()),
            S3Step.S3_STEP
        );
    }

    @Override
    public void onConfigurationStepConfigured(final String stepName) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow(getInitializationContext().getConfigurationContext());
        getInitializationContext().updateFlow(flow);
    }

    @Override
    public void abortUpdatePreparation(final Throwable throwable) {
    }

    @Override
    public void finishUpdate() throws FlowUpdateException {
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
        final List<ValidationResult> results = new ArrayList<>();

        // Validate Connectivity
        if (stepName.equals(KafkaConnectionStep.STEP_NAME)) {
            // Get the current ConfigurationContext and then create a new one that contains the provided property values
            final ConnectorConfigurationContext configurationContext = getInitializationContext().getConfigurationContext().createWithOverrides(stepName, propertyValues);

            // Build a new version of the flow so that we can get the relevant properties of the Kafka Connection Service
            final VersionedExternalFlow flow = buildFlow(configurationContext);

            final ControllerServiceFacade connectionService = getKafkaConnectionService();
            final List<ConfigVerificationResult> configVerificationResults = connectionService.verify(flow, Map.of());
            for (final ConfigVerificationResult result : configVerificationResults) {
                if (result.getOutcome() == Outcome.FAILED) {
                    results.add(new ValidationResult.Builder()
                        .subject("Kafka Connection")
                        .valid(false)
                        .explanation(result.getExplanation())
                        .build());
                }
            }
        }

        return results;
    }

    private VersionedExternalFlow buildFlow(final ConnectorConfigurationContext configurationContext) {
        final KafkaToS3FlowBuilder flowBuilder = new KafkaToS3FlowBuilder(configurationContext);
        return flowBuilder.buildFlow();
    }

    @SuppressWarnings("unchecked")
    private List<String> getAvailableTopics() {
        final ControllerServiceFacade kafkaConnectionService = getKafkaConnectionService();

        try {
            return (List<String>) kafkaConnectionService.invokeConnectorMethod("listTopicNames", Map.of());
        } catch (final Exception e) {
            getLogger().warn("Failed to retrieve available Kafka topics", e);
            return List.of();
        }
    }

    private ControllerServiceFacade getKafkaConnectionService() {
        return getInitializationContext().getRootGroup().getControllerServices().stream()
            .filter(service -> service.getDefinition().getType().endsWith("Kafka3ConnectionService"))
            .findFirst()
            .orElseThrow();
    }
}
