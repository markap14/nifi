/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceHierarchy;
import org.apache.nifi.components.connector.components.ControllerServiceReferenceScope;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@CapabilityDescription("Provides the ability to ingest data from Apache Kafka topics, merge it together into an object of reasonable " +
                       "size, and write that data to Amazon S3.")
@Tags({"kafka", "s3"})
public class KafkaToS3 extends AbstractConnector {

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(
            KafkaConnectionStep.KAFKA_CONNECTION_STEP,
            KafkaTopicsStep.createConfigurationStep(getAvailableTopics()),
            S3Step.createConfigurationStep(getPossibleS3Regions())
        );
    }

    @Override
    protected void init() throws FlowUpdateException {
        if (getInitializationContext().getRootGroup().isFlowEmpty()) {
            getInitializationContext().updateFlow(KafkaToS3FlowBuilder.loadInitialFlow());
        }
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
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValues) {
        // Get the current ConfigurationContext and then create a new one that contains the provided property values
        final ConnectorConfigurationContext configurationContext = getInitializationContext().getConfigurationContext().createWithOverrides(stepName, propertyValues);
        final VersionedExternalFlow flow = buildFlow(configurationContext);

        // Validate Connectivity
        if (stepName.equals(KafkaConnectionStep.STEP_NAME)) {
            return verifyKafkaConnectivity(flow);
        }
        if (stepName.equals(KafkaTopicsStep.STEP_NAME)) {
            final List<ConfigVerificationResult> results = new ArrayList<>();
            results.addAll(verifyTopicsExists(configurationContext));
            results.addAll(verifyKafkaParsability(flow));
            return results;
        }

        return Collections.emptyList();
    }

    private List<ConfigVerificationResult> verifyKafkaParsability(final VersionedExternalFlow flow) {
        // Enable Controller Services necessary for parsing records.
        // We determine which Controller Services are referenced by the flow and enable them, but we do not use
        // getRootGroup().getLifecycle().enableReferencedControllerServices(ControllerServiceReferenceScope.INCLUDE_REFERENCED_SERVICES_ONLY)
        // because that would include the Controller Services that are referenced by the currently configured flow, and it's possible that the
        // what is being verified uses a different set of Controller Services (e.g., the verified flow may use a JSON Reader while the current
        // flow uses an Avro Reader).
        final Set<VersionedControllerService> referencedServices = VersionedFlowUtils.getReferencedControllerServices(flow.getFlowContents());
        final Set<String> serviceIds = referencedServices.stream()
            .map(VersionedControllerService::getIdentifier)
            .collect(Collectors.toSet());

        try {
            getInitializationContext().getRootGroup().getLifecycle().enableControllerServices(serviceIds).get(10, TimeUnit.SECONDS);
        } catch (final Exception e) {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Record Parsing")
                .outcome(Outcome.FAILED)
                .explanation("Failed to enable Controller Services due to " + e)
                .build());
        }

        try {
            final ProcessorFacade consumeKafkaFacade = findProcessors(getInitializationContext().getRootGroup(),
                processor -> processor.getDefinition().getType().endsWith("ConsumeKafka")).getFirst();

            final List<ConfigVerificationResult> configVerificationResults = consumeKafkaFacade.verify(flow, Map.of());
            for (final ConfigVerificationResult result : configVerificationResults) {
                if (result.getOutcome() == Outcome.FAILED) {
                    return List.of(result);
                }
            }

            return Collections.emptyList();
        } finally {
            getInitializationContext().getRootGroup().getLifecycle().disableControllerServices(serviceIds);
        }
    }


    @SuppressWarnings("unchecked")
    private List<ConfigVerificationResult> verifyTopicsExists(ConnectorConfigurationContext configurationContext) {
        final List<String> topicsAvailable;
        try {
            topicsAvailable = getAvailableTopics();
        } catch (final Exception e) {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Kafka topics exist")
                .outcome(Outcome.SKIPPED)
                .explanation("Unable to validate that topics exist due to " + e)
                .build());
        }

        final Set<String> topicNames = new HashSet<>(topicsAvailable);
        final List<String> specifiedTopics = configurationContext.getProperty(KafkaTopicsStep.STEP_NAME, KafkaTopicsStep.TOPIC_NAMES.getName()).asList();
        final String missingTopics = specifiedTopics.stream()
            .filter(topic -> !topicNames.contains(topic))
            .collect(Collectors.joining(", "));

        if (!missingTopics.isEmpty()) {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Kafka topics exist")
                .outcome(Outcome.FAILED)
                .explanation("The following topics do not exist in the Kafka cluster: " + missingTopics)
                .build());
        } else {
            return List.of(new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Kafka topics exist")
                .outcome(Outcome.SUCCESSFUL)
                .explanation("All specified topics exist in the Kafka cluster")
                .build());
        }
    }

    private List<ConfigVerificationResult> verifyKafkaConnectivity(final VersionedExternalFlow flow) {
        // Build a new version of the flow so that we can get the relevant properties of the Kafka Connection Service
        final ControllerServiceFacade connectionService = getKafkaConnectionService();
        final List<ConfigVerificationResult> configVerificationResults = connectionService.verify(flow, Map.of());

        for (final ConfigVerificationResult result : configVerificationResults) {
            if (result.getOutcome() == Outcome.FAILED) {
                return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName("Verify Kafka connectivity")
                    .outcome(Outcome.FAILED)
                    .explanation(result.getExplanation())
                    .build());
            }
        }

        return Collections.emptyList();
    }

    private VersionedExternalFlow buildFlow(final ConnectorConfigurationContext configurationContext) {
        final KafkaToS3FlowBuilder flowBuilder = new KafkaToS3FlowBuilder(configurationContext);
        return flowBuilder.buildFlow();
    }

    @SuppressWarnings("unchecked")
    private List<String> getAvailableTopics() {
        // If Kafka Brokers not yet set, return empty list
        if (!getProperty(KafkaConnectionStep.KAFKA_CONNECTION_STEP, KafkaConnectionStep.KAFKA_BROKERS).isSet()) {
            return List.of();
        }

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

    @SuppressWarnings("unchecked")
    private List<String> getPossibleS3Regions() {
        final ProcessorFacade processorFacade = getInitializationContext().getRootGroup().getProcessors().stream()
            .filter(proc -> proc.getDefinition().getType().endsWith("PutS3Object"))
            .findFirst()
            .orElseThrow();

        try {
            return (List<String>) processorFacade.invokeConnectorMethod("getAvailableRegions", Map.of());
        } catch (final InvocationFailedException e) {
            getLogger().error("Failed to obtain list of available S3 regions", e);
            return Collections.emptyList();
        }
    }
}
