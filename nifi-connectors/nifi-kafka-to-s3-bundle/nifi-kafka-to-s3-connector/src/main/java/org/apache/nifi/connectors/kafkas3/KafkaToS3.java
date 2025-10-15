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
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: Rename ConnectorUtils to VersionedFlowUtils
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
    public void finishUpdate() {
    }

    @Override
    public List<ValidationResult> validateConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
        // Get the current ConfigurationContext and then create a new one that contains the provided property values
        final ConnectorConfigurationContext configurationContext = getInitializationContext().getConfigurationContext().createWithOverrides(stepName, propertyValues);
        final VersionedExternalFlow flow = buildFlow(configurationContext);

        // Validate Connectivity
        if (stepName.equals(KafkaConnectionStep.STEP_NAME)) {
            return verifyKafkaConnectivity(configurationContext, flow);
        }
        if (stepName.equals(KafkaTopicsStep.STEP_NAME)) {
            final List<ValidationResult> results = new ArrayList<>();
            results.addAll(verifyKafkaParsability(configurationContext, flow));
            results.addAll(verifyTopicsExists(configurationContext));
            return results;
        }

        return List.of();
    }

    private List<ValidationResult> verifyKafkaParsability(final ConnectorConfigurationContext configurationContext, final VersionedExternalFlow flow) {
        final ProcessorFacade consumeKafkaFacade = findProcessors(getInitializationContext().getRootGroup(),
            processor -> processor.getDefinition().getType().endsWith("ConsumeKafka")).getFirst();

        final List<ConfigVerificationResult> configVerificationResults = consumeKafkaFacade.verify(flow, Map.of());
        for (final ConfigVerificationResult result : configVerificationResults) {
            if (result.getOutcome() == Outcome.FAILED) {
                return List.of(createValidationResult("Kafka Connection", result).orElseThrow());
            }
        }

        return List.of();
    }

    @SuppressWarnings("unchecked")
    private List<ValidationResult> verifyTopicsExists(ConnectorConfigurationContext configurationContext) {
        final ControllerServiceFacade connectionService = getKafkaConnectionService();

        final List<String> topicsAvailable;
        try {
            topicsAvailable = (List<String>) connectionService.invokeConnectorMethod("listTopicNames", Map.of());
        } catch (final Exception e) {
            return List.of(new ValidationResult.Builder()
                .subject("Kafka Topics")
                .valid(false)
                .explanation("Failed to retrieve available topics from Kafka: " + e)
                .build());
        }

        final Set<String> topicNames = new HashSet<>(topicsAvailable);
        final List<String> specifiedTopics = configurationContext.getProperty(KafkaTopicsStep.STEP_NAME, KafkaTopicsStep.TOPIC_NAMES.getName()).asList();
        final String missingTopics = specifiedTopics.stream()
            .filter(topic -> !topicNames.contains(topic))
            .collect(Collectors.joining(", "));

        if (!missingTopics.isEmpty()) {
            return List.of(new ValidationResult.Builder()
                .subject("Kafka Topics")
                .valid(false)
                .explanation("The following topics do not exist in the Kafka cluster: " + missingTopics)
                .build());
        } else {
            return List.of(new ValidationResult.Builder()
                .subject("Kafka Topics")
                .valid(true)
                .explanation("All specified topics exist in the Kafka cluster")
                .build());
        }
    }

    private List<ValidationResult> verifyKafkaConnectivity(final ConnectorConfigurationContext configurationContext, final VersionedExternalFlow flow) {
        // Build a new version of the flow so that we can get the relevant properties of the Kafka Connection Service
        final ControllerServiceFacade connectionService = getKafkaConnectionService();
        final List<ConfigVerificationResult> configVerificationResults = connectionService.verify(flow, Map.of());

        for (final ConfigVerificationResult result : configVerificationResults) {
            if (result.getOutcome() == Outcome.FAILED) {
                return List.of(createValidationResult("Kafka Connection", result).orElseThrow());
            }
        }

        return List.of();
    }


    private Optional<ValidationResult> createValidationResult(final String subject, final ConfigVerificationResult result) {
        if (result.getOutcome() == Outcome.SKIPPED) {
            return Optional.empty();
        }

        final ValidationResult validationResult = new ValidationResult.Builder()
            .subject(subject)
            .valid(result.getOutcome() == Outcome.SUCCESSFUL)
            .explanation(result.getExplanation())
            .build();

        return Optional.of(validationResult);
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
