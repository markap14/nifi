/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyGroupConfiguration;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaToS3IT {

    private static ConnectorTestRunner runner;
    private static ConfluentKafkaContainer kafkaContainer;
    private static LocalStackContainer localStackContainer;
    private static S3Client s3Client;

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
    public static void setupTestContainers() {
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

        s3Client = S3Client.builder()
            .endpointOverride(localStackContainer.getEndpoint())
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
            .region(Region.of(S3_REGION))
            .httpClient(UrlConnectionHttpClient.builder().build())
            .forcePathStyle(true)
            .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_BUCKET_NAME).build());
    }

    @BeforeEach
    public void setupRunner() {
        runner = new StandardConnectorTestRunner.Builder()
            .connectorClassName("org.apache.nifi.connectors.kafkas3.KafkaToS3")
            .narLibraryDirectory(new File("target/libDir"))
            .build();
        assertNotNull(runner);
    }

    @AfterAll
    public static void cleanupTestContainers() throws IOException {
        if (s3Client != null) {
            s3Client.close();
        }

        if (localStackContainer != null) {
            localStackContainer.stop();
        }

        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    @AfterEach
    public void cleanupRunner() throws IOException {
        if (runner != null) {
            runner.close();
        }
    }


    private void createKafkaTopics(final String... topicNames) throws ExecutionException, InterruptedException {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        adminProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        adminProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        adminProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            SCRAM_USERNAME, SCRAM_PASSWORD
        ));

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final List<NewTopic> topics = new ArrayList<>();
            for (final String topicName : topicNames) {
                topics.add(new NewTopic(topicName, 1, (short) 1));
            }

            adminClient.createTopics(topics).all().get();
        }
    }

    private void produceRecordsToTopic(final String topicName, final String... records) throws ExecutionException, InterruptedException {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            SCRAM_USERNAME, SCRAM_PASSWORD
        ));

        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (final String record : records) {
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, record);
                producer.send(producerRecord).get();
            }

            producer.flush();
        }
    }


    @Test
    public void testVerification() throws ExecutionException, InterruptedException, FlowUpdateException {
        createKafkaTopics("topic-1", "topic-2", "topic-3", "topic-4", "topic-5", "Z-topic", "an-important-topic");

        produceRecordsToTopic("topic-1",
            """
            {"id": 1, "name": "Alice", "age": 30}""",
            """
            {"id": 2, "name": "Bob", "age": 25}""",
            """
            {"id": 3, "name": "Charlie", "age": 35}"""
        );

        produceRecordsToTopic("an-important-topic",
            "This is a plaintext message",
            "Another important message",
            "Final plaintext record"
        );

        final PropertyGroupConfiguration kafkaServerConfig = new PropertyGroupConfiguration("Kafka Server Settings", Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        ));

        // TODO: Add assertions to framework: runner.assertConfigurationValid(...)
        // TODO: Add ability to fetch allowable values from Connector
        runner.prepareForUpdate();

        // Perform verification to ensure that valid server configuration passes
        final List<ConfigVerificationResult> connectionVerificationResults = runner.verifyConfiguration("Kafka Connection", List.of(kafkaServerConfig));
        assertEquals(List.of(), connectionVerificationResults);

        // Apply the configuration that we've now validated
        runner.configure("Kafka Connection", List.of(kafkaServerConfig));

        // Perform verification to ensure that valid topic configuration passes
        final PropertyGroupConfiguration topic1Config = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "topic-1",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        ));
        final List<ConfigVerificationResult> topic1VerificationResults = runner.verifyConfiguration("Kafka Topics", List.of(topic1Config));
        assertEquals(List.of(), topic1VerificationResults.stream().filter(result -> result.getOutcome() == Outcome.FAILED).toList());

        // Perform verification against a topic with invalid data for the selected data format
        final PropertyGroupConfiguration importantTopicConfig = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "an-important-topic",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        ));

        final List<ConfigVerificationResult> importantTopicVerificationResults = runner.verifyConfiguration("Kafka Topics", List.of(importantTopicConfig));
        final List<ConfigVerificationResult> invalidImportantTopicResults = importantTopicVerificationResults.stream()
            .filter(result -> result.getOutcome() == Outcome.FAILED)
            .toList();
        assertEquals(1, invalidImportantTopicResults.size());
        final ConfigVerificationResult invalidResult = invalidImportantTopicResults.getFirst();
        assertTrue(invalidResult.getExplanation().contains("parse"), "Unexpected validation reason: " + invalidResult.getExplanation());

        runner.finishUpdate();
    }

    @Test
    public void testSuccessfulFlow() throws IOException, ExecutionException, InterruptedException, FlowUpdateException {
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

        final PropertyGroupConfiguration kafkaServerConfig = new PropertyGroupConfiguration("Kafka Server Settings", Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        ));

        final PropertyGroupConfiguration kafkaTopicConfig = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "story",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        ));

        final PropertyGroupConfiguration s3DestinationConfig = new PropertyGroupConfiguration("S3 Destination Configuration", Map.of(
            "S3 Region", S3_REGION,
            "S3 Data Format", "Avro",
            "S3 Bucket", S3_BUCKET_NAME,
            "S3 Endpoint Override URL", localStackContainer.getEndpoint().toString()
        ));
        final PropertyGroupConfiguration s3CredentialsConfig = new PropertyGroupConfiguration("S3 Credentials", Map.of(
            "S3 Authentication Strategy", "Static Credentials",
            "Access Key ID", localStackContainer.getAccessKey(),
            "Secret Access Key", localStackContainer.getSecretKey()
        ));
        final PropertyGroupConfiguration s3MergeConfig = new PropertyGroupConfiguration("Merge Configuration", Map.of(
            "Target Object Size", "1 MB",
            "Merge Latency", "5 sec"
        ));

        runner.prepareForUpdate();
        runner.configure("Kafka Connection", List.of(kafkaServerConfig));
        runner.configure("Kafka Topics", List.of(kafkaTopicConfig));
        runner.configure("S3 Configuration", List.of(s3DestinationConfig, s3MergeConfig, s3CredentialsConfig));
        runner.finishUpdate();

        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(Collections.emptyList(), validationResults);

        runner.startConnector();
        try {
            runner.waitForDataIngested(Duration.ofSeconds(10));
            runner.waitForIdle(Duration.ofSeconds(30));
        } finally {
            runner.stopConnector();
        }

        verifyS3ObjectsCreated();
    }

    private void verifyS3ObjectsCreated() throws IOException {
        final ListObjectsV2Response listResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(S3_BUCKET_NAME).build());
        final List<S3Object> objects = listResponse.contents();

        assertFalse(objects.isEmpty(), "Expected at least one object in S3 bucket");

        for (final S3Object s3Object : objects) {
            final GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(S3_BUCKET_NAME).key(s3Object.key()).build();
            try (final ResponseInputStream<GetObjectResponse> objectContent = s3Client.getObject(getObjectRequest)) {
                final long objectSize = objectContent.response().contentLength();
                assertTrue(objectSize > 0, "Expected S3 object " + s3Object.key() + " to have content");
            }
        }
    }

}
