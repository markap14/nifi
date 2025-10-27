/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KafkaToS3IT {

    private static ConnectorTestRunner runner;

    private static ConfluentKafkaContainer kafkaContainer;

    private static final String SCRAM_USERNAME = "testuser";
    private static final String SCRAM_PASSWORD = "testpassword";

    // JAAS configuration for Kafka broker SASL/PLAIN authentication.
    // The 'username' and 'password' fields are credentials the broker uses for inter-broker communication.
    // The 'user_<username>="<password>"' entries define client users that can authenticate to this broker.
    // In this setup:
    //   - Broker uses 'admin' / 'admin-secret' for inter-broker communication (though we use PLAINTEXT for that)
    //   - Clients can authenticate using 'testuser' / 'testpassword' on the SASL listener with PLAIN mechanism
    private static final String JAAS_CONFIG_CONTENT = """
        KafkaServer {
          org.apache.kafka.common.security.plain.PlainLoginModule required
          username="admin"
          password="admin-secret"
          user_%s="%s";
        };
        """.formatted(SCRAM_USERNAME, SCRAM_PASSWORD);


    @BeforeAll
    public static void setupTestRunner() {
        runner = new StandardConnectorTestRunner.Builder()
            .connectorClassName("org.apache.nifi.connectors.kafkas3.KafkaToS3")
            .narLibraryDirectory(new File("target/libDir"))
            .build();
        assertNotNull(runner);

        kafkaContainer = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));
        kafkaContainer
            .withStartupTimeout(Duration.ofSeconds(10))
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT")
            .withEnv("KAFKA_LISTENERS", "CONTROLLER://0.0.0.0:9094,BROKER://0.0.0.0:9092,PLAINTEXT://0.0.0.0:19092,SASL://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://localhost:9092,PLAINTEXT://localhost:19092,SASL://localhost:9093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_jaas.conf")
            .withCommand(
                "sh", "-c",
                "echo '" + JAAS_CONFIG_CONTENT + "' > /tmp/kafka_jaas.conf && " +
                "/etc/confluent/docker/run"
            )
            .setPortBindings(List.of("9093:9093"));

        kafkaContainer.start();
    }

    @AfterAll
    public static void cleanup() throws IOException {
        if (runner != null) {
            runner.close();
        }

        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    private void createKafkaTopics(final String... topicNames) throws IOException, InterruptedException {
        for (final String topicName : topicNames) {
            kafkaContainer.execInContainer(
                "kafka-topics",
                "--create",
                "--topic", topicName,
                "--bootstrap-server", "localhost:9092",
                "--partitions", "1",
                "--replication-factor", "1"
            );
        }
    }

    private void produceRecordsToTopic(final String topicName, final String... records) throws IOException, InterruptedException {
        final String recordsData = String.join("\n", records);
        final ExecResult result = kafkaContainer.execInContainer(
            "sh", "-c",
            "echo '" + recordsData + "' | kafka-console-producer --bootstrap-server localhost:9092 --topic " + topicName
        );

        assertEquals(0, result.getExitCode());
    }


    @Test
    public void testValidate() {
        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(List.of(), validationResults);
    }

    @Test
    public void testSuccessfulFlow() throws IOException, InterruptedException, FlowUpdateException {
        createKafkaTopics("story");

        produceRecordsToTopic("story",
            """
            {"page": 1, "words": "Once upon a time, there was a NiFi developer." }""",
            """
            {"page": 2, "words": "The developer wanted to build a connector to move data from Kafka to S3." }""",
            """
            {"page": 3, "words": "After much effort, the connector was complete and worked flawlessly!" }""",
            """
            {"page": 4, "words": "The end." }"""
        );

        runner.prepareForUpdate();
        runner.configure("Kafka Connection", "Kafka Server Settings", Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        ));
        runner.configure("Kafka Topics", "Kafka Topics Configuration", Map.of(
            "Topic Names", "story",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        ));
        runner.configure("S3 Configuration", "S3 Destination Configuration", Map.of(
            "S3 Region", "us-west-2",
            "S3 Data Format", "Avro",
            "S3 Bucket", "mpayne-test-bucket-123"
        ));
        runner.configure("S3 Configuration", "Merge Configuration", Map.of(
            "Target Object Size", "1 MB",
            "Merge Latency", "5 sec"
        ));
        runner.configure("S3 Configuration", "S3 Credentials", Map.of(
            "S3 Authentication Strategy", "Default AWS Credentials"
        ));
        runner.finishUpdate();

        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(Collections.emptyList(), validationResults);

        runner.startConnector();
        runner.waitForDataIngested(Duration.ofSeconds(10));
        runner.waitForIdle(Duration.ofSeconds(5), Duration.ofSeconds(30));
        runner.stopConnector();
    }

}
