/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

public class KafkaToS3FlowBuilder {
    private static final String FLOW_JSON_PATH = "/flows/Kafka_to_S3.json";

    private final ConnectorConfigurationContext configContext;

    public KafkaToS3FlowBuilder(final ConnectorConfigurationContext configurationContext) {
        this.configContext = configurationContext;
    }

    public VersionedExternalFlow buildFlow() {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource(FLOW_JSON_PATH);
        configureSchemaRegistry(externalFlow);

        updateKafkaConnectionParameters(externalFlow);
        updateSchemaRegistryParameters(externalFlow);
        updateReaderWriterParameters(externalFlow);
        updateS3Config(externalFlow);

        return externalFlow;
    }

    private void configureSchemaRegistry(final VersionedExternalFlow externalFlow) {
        final ConfigurationStep connectionStep = KafkaConnectionStep.KAFKA_CONNECTION_STEP;
        final String schemaRegistryUrl = configContext.getProperty(connectionStep, KafkaConnectionStep.SCHEMA_REGISTRY_URL).getValue();
        if (schemaRegistryUrl == null) {
            final VersionedProcessGroup processGroup = externalFlow.getFlowContents();

            // Remove any references to the Schema Registry service.
            final VersionedControllerService schemaRegistryService = processGroup.getControllerServices().stream()
                .filter(service -> service.getType().endsWith("ConfluentSchemaRegistry"))
                .findFirst()
                .orElseThrow();

            processGroup.getControllerServices().remove(schemaRegistryService);
            VersionedFlowUtils.removeControllerServiceReferences(processGroup, schemaRegistryService.getIdentifier());
        }
    }

    private void updateSchemaRegistryParameters(final VersionedExternalFlow externalFlow) {
        final ConfigurationStep connectionStep = KafkaConnectionStep.KAFKA_CONNECTION_STEP;

        final String schemaRegistryUrl = configContext.getProperty(connectionStep, KafkaConnectionStep.SCHEMA_REGISTRY_URL).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry URLs", schemaRegistryUrl);
        if (schemaRegistryUrl != null) {
            final String username = configContext.getProperty(connectionStep, KafkaConnectionStep.SCHEMA_REGISTRY_USERNAME).getValue();
            final String password = configContext.getProperty(connectionStep, KafkaConnectionStep.PASSWORD).getValue();

            VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry Username", username);
            VersionedFlowUtils.setParameterValue(externalFlow, "Schema Registry Password", password);
        }
    }

    private void updateKafkaConnectionParameters(final VersionedExternalFlow externalFlow) {
        final ConfigurationStep connectionStep = KafkaConnectionStep.KAFKA_CONNECTION_STEP;

        final String kafkaBrokers = configContext.getProperty(connectionStep, KafkaConnectionStep.KAFKA_BROKERS).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Bootstrap Servers", kafkaBrokers);

        final String securityProtocol = configContext.getProperty(connectionStep, KafkaConnectionStep.SECURITY_PROTOCOL).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Security Protocol", securityProtocol);

        if (securityProtocol.contains("SASL")) {
            final String saslMechanism = configContext.getProperty(connectionStep, KafkaConnectionStep.SASL_MECHANISM).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "Kafka SASL Mechanism", saslMechanism);

            final String username = configContext.getProperty(connectionStep, KafkaConnectionStep.USERNAME).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Username", username);

            final String password = configContext.getProperty(connectionStep, KafkaConnectionStep.PASSWORD).getValue();
            VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Password", password);
        }
    }

    private void updateReaderWriterParameters(final VersionedExternalFlow externalFlow) {
        final String kafkaDataFormat = configContext.getProperty(KafkaTopicsStep.STEP_NAME, KafkaTopicsStep.KAFKA_DATA_FORMAT.getName()).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "Kafka Data Format", kafkaDataFormat);

        final String s3DataFormat = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_DATA_FORMAT).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Data Format", s3DataFormat);
    }

    private void updateS3Config(final VersionedExternalFlow externalFlow) {
        final String region = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_REGION).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Region", region);

        final String bucket = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_BUCKET).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Bucket", bucket);

        final String prefix = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_PREFIX).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Prefix", prefix);

        final String accessKey = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_ACCESS_KEY_ID).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Access Key ID", accessKey);

        final String secretKey = configContext.getProperty(S3Step.S3_STEP, S3Step.S3_SECRET_ACCESS_KEY).getValue();
        VersionedFlowUtils.setParameterValue(externalFlow, "S3 Secret Access Key", secretKey);
    }
}
