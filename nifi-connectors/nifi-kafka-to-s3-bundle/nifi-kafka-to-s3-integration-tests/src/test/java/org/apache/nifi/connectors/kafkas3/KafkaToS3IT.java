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
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaToS3IT {

    private static ConnectorTestRunner runner;

    private static ConfluentKafkaContainer kafkaContainer;

    private static LocalStackContainer localStackContainer;

    private static final String SCRAM_USERNAME = "testuser";
    private static final String SCRAM_PASSWORD = "testpassword";

    private static final String S3_BUCKET_NAME = "test-bucket";
    private static final String S3_REGION = "us-west-2";

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
    public static void setupTestRunner() throws IOException, InterruptedException {
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

        localStackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.1.0"))
            .withServices(LocalStackContainer.Service.S3)
            .withStartupTimeout(Duration.ofSeconds(30));

        localStackContainer.start();

        createS3Bucket();
    }

    @AfterAll
    public static void cleanup() throws IOException {
        if (runner != null) {
            runner.close();
        }

        if (localStackContainer != null) {
            localStackContainer.stop();
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

    private static void createS3Bucket() throws IOException, InterruptedException {
        final ExecResult result = localStackContainer.execInContainer(
            "awslocal", "s3", "mb", "s3://" + S3_BUCKET_NAME, "--region", S3_REGION
        );

        assertEquals(0, result.getExitCode(), "Failed to create S3 bucket: " + result.getStderr());
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
            "S3 Region", S3_REGION,
            "S3 Data Format", "Avro",
            "S3 Bucket", S3_BUCKET_NAME,
            "S3 Endpoint Override URL", localStackContainer.getEndpoint().toString()
        ));
        runner.configure("S3 Configuration", "Merge Configuration", Map.of(
            "Target Object Size", "1 MB",
            "Merge Latency", "5 sec"
        ));
        runner.configure("S3 Configuration", "S3 Credentials", Map.of(
            "S3 Authentication Strategy", "Static Credentials",
            "Access Key ID", localStackContainer.getAccessKey(),
            "Secret Access Key", localStackContainer.getSecretKey()
        ));
        runner.finishUpdate();

        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(Collections.emptyList(), validationResults);

        runner.startConnector();
        runner.waitForDataIngested(Duration.ofSeconds(10));
        runner.waitForIdle(Duration.ofSeconds(5), Duration.ofSeconds(30));
        runner.stopConnector();

        verifyS3ObjectsCreated();
    }

    private void verifyS3ObjectsCreated() throws IOException, InterruptedException {
        final ExecResult listResult = localStackContainer.execInContainer(
            "awslocal", "s3", "ls", "s3://" + S3_BUCKET_NAME + "/", "--region", S3_REGION
        );

        assertEquals(0, listResult.getExitCode(), "Failed to list S3 objects: " + listResult.getStderr());

        final String stdout = listResult.getStdout();
        assertFalse(stdout.trim().isEmpty(), "Expected at least one object in S3 bucket");

        final String[] lines = stdout.trim().split("\n");
        assertTrue(lines.length > 0, "Expected at least one object in S3 bucket");

        for (final String line : lines) {
            final String[] parts = line.trim().split("\\s+");
            assertTrue(parts.length >= 4, "Expected S3 object listing to have at least 4 parts: " + line);
            final String sizeStr = parts[2];
            final long size = Long.parseLong(sizeStr);
            assertTrue(size > 0, "Expected S3 object to have size greater than 0, but was: " + size);
        }
    }

}
