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

package org.apache.nifi.connectors.kafkas3;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyGroupConfiguration;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorConfigVerificationResult;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
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
    private static Network network;
    private static ConfluentKafkaContainer kafkaContainer;
    private static GenericContainer<?> schemaRegistryContainer;
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
        network = Network.newNetwork();

        kafkaContainer = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withStartupTimeout(Duration.ofSeconds(10))
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT")
            .withEnv("KAFKA_LISTENERS", "CONTROLLER://0.0.0.0:9094,BROKER://0.0.0.0:9092,PLAINTEXT://0.0.0.0:19092,SASL://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://kafka:9092,PLAINTEXT://kafka:19092,SASL://localhost:9093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
            .withEnv("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_jaas.conf")
            .withCommand(
                "sh", "-c",
                "echo '" + JAAS_CONFIG_CONTENT + "' > /tmp/kafka_jaas.conf && " +
                "/etc/confluent/docker/run"
            );

        kafkaContainer.setPortBindings(List.of("9093:9093"));
        kafkaContainer.start();

        schemaRegistryContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0"))
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withStartupTimeout(Duration.ofSeconds(60))
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .dependsOn(kafkaContainer);

        schemaRegistryContainer.start();

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
    public static void cleanupTestContainers() {
        if (s3Client != null) {
            s3Client.close();
        }

        if (localStackContainer != null) {
            localStackContainer.stop();
        }

        if (schemaRegistryContainer != null) {
            schemaRegistryContainer.stop();
        }

        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }

        if (network != null) {
            network.close();
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

    private String getSchemaRegistryUrl() {
        return String.format("http://%s:%d", schemaRegistryContainer.getHost(), schemaRegistryContainer.getMappedPort(8081));
    }

    private void produceAvroRecordsToTopic(final String topicName, final Schema schema, final GenericRecord... records) throws ExecutionException, InterruptedException {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            SCRAM_USERNAME, SCRAM_PASSWORD
        ));
        producerProps.put("schema.registry.url", getSchemaRegistryUrl());

        try (final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
            for (final GenericRecord record : records) {
                final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topicName, record);
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

        runner.prepareForUpdate();

        // Perform verification to ensure that valid server configuration passes
        final ConnectorConfigVerificationResult connectionVerificationResults = runner.verifyConfiguration("Kafka Connection", List.of(kafkaServerConfig));
        connectionVerificationResults.assertNoFailures();

        // Apply the configuration that we've now validated
        runner.configure("Kafka Connection", List.of(kafkaServerConfig));

        // Perform verification to ensure that valid topic configuration passes
        final PropertyGroupConfiguration topic1Config = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "topic-1",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        ));
        final ConnectorConfigVerificationResult topic1VerificationResults = runner.verifyConfiguration("Kafka Topics", List.of(topic1Config));
        topic1VerificationResults.assertNoFailures();

        // Perform verification against a topic with invalid data for the selected data format
        final PropertyGroupConfiguration importantTopicConfig = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "an-important-topic",
            "Consumer Group ID", "nifi-kafka-to-s3-testSuccessfulFlow",
            "Offset Reset", "earliest",
            "Kafka Data Format", "JSON"
        ));

        final ConnectorConfigVerificationResult importantTopicVerificationResults = runner.verifyConfiguration("Kafka Topics", List.of(importantTopicConfig));
        final List<ConfigVerificationResult> invalidImportantTopicResults = importantTopicVerificationResults.getFailedResults();
        assertEquals(1, invalidImportantTopicResults.size());
        final ConfigVerificationResult invalidResult = invalidImportantTopicResults.getFirst();
        assertTrue(invalidResult.getExplanation().contains("parse"), "Unexpected validation reason: " + invalidResult.getExplanation());

        runner.finishUpdate();
    }

    @Test
    public void testJsonFlow() throws IOException, ExecutionException, InterruptedException, FlowUpdateException {
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

    @Test
    public void testSchemaRegistryVerification() throws ExecutionException, InterruptedException, FlowUpdateException {
        createKafkaTopics("avro-topic");

        final String schemaString = """
            {
              "type": "record",
              "name": "TestRecord",
              "namespace": "org.apache.nifi.test",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "message", "type": "string"}
              ]
            }""";

        final Schema schema = new Schema.Parser().parse(schemaString);

        final GenericRecord record1 = new GenericData.Record(schema);
        record1.put("id", 100);
        record1.put("message", "Test message 1");

        final GenericRecord record2 = new GenericData.Record(schema);
        record2.put("id", 200);
        record2.put("message", "Test message 2");

        produceAvroRecordsToTopic("avro-topic", schema, record1, record2);

        final PropertyGroupConfiguration kafkaServerConfig = new PropertyGroupConfiguration("Kafka Server Settings", Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        ));

        final PropertyGroupConfiguration schemaRegistryConfig = new PropertyGroupConfiguration("Schema Registry Settings", Map.of(
            "Schema Registry URL", getSchemaRegistryUrl()
        ));

        runner.prepareForUpdate();

        final ConnectorConfigVerificationResult connectionVerificationResults = runner.verifyConfiguration("Kafka Connection", List.of(kafkaServerConfig, schemaRegistryConfig));
        connectionVerificationResults.assertNoFailures();

        runner.configure("Kafka Connection", List.of(kafkaServerConfig, schemaRegistryConfig));

        final PropertyGroupConfiguration avroTopicConfig = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "avro-topic",
            "Consumer Group ID", "nifi-kafka-to-s3-testSchemaRegistryVerification",
            "Offset Reset", "earliest",
            "Kafka Data Format", "Avro"
        ));

        final ConnectorConfigVerificationResult avroTopicVerificationResults = runner.verifyConfiguration("Kafka Topics", List.of(avroTopicConfig));
        avroTopicVerificationResults.assertNoFailures();

        runner.finishUpdate();
    }

    @Test
    public void testWithSchemaRegistry() throws IOException, ExecutionException, InterruptedException, FlowUpdateException {
        createKafkaTopics("user-events");

        final String schemaString = """
            {
              "type": "record",
              "name": "UserEvent",
              "namespace": "org.apache.nifi.test",
              "fields": [
                {"name": "userId", "type": "int"},
                {"name": "userName", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "timestamp", "type": "long"}
              ]
            }""";

        final Schema schema = new Schema.Parser().parse(schemaString);

        final GenericRecord record1 = new GenericData.Record(schema);
        record1.put("userId", 1001);
        record1.put("userName", "alice");
        record1.put("eventType", "login");
        record1.put("timestamp", System.currentTimeMillis());

        final GenericRecord record2 = new GenericData.Record(schema);
        record2.put("userId", 1002);
        record2.put("userName", "bob");
        record2.put("eventType", "purchase");
        record2.put("timestamp", System.currentTimeMillis());

        final GenericRecord record3 = new GenericData.Record(schema);
        record3.put("userId", 1003);
        record3.put("userName", "charlie");
        record3.put("eventType", "logout");
        record3.put("timestamp", System.currentTimeMillis());

        produceAvroRecordsToTopic("user-events", schema, record1, record2, record3);

        final PropertyGroupConfiguration kafkaServerConfig = new PropertyGroupConfiguration("Kafka Server Settings", Map.of(
            "Kafka Brokers", "localhost:9093",
            "Security Protocol", "SASL_PLAINTEXT",
            "SASL Mechanism", "PLAIN",
            "Username", SCRAM_USERNAME,
            "Password", SCRAM_PASSWORD
        ));

        final PropertyGroupConfiguration schemaRegistryConfig = new PropertyGroupConfiguration("Schema Registry Settings", Map.of(
            "Schema Registry URL", getSchemaRegistryUrl()
        ));

        final PropertyGroupConfiguration kafkaTopicConfig = new PropertyGroupConfiguration("Kafka Topics Configuration", Map.of(
            "Topic Names", "user-events",
            "Consumer Group ID", "nifi-kafka-to-s3-testSchemaRegistry",
            "Offset Reset", "earliest",
            "Kafka Data Format", "Avro"
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
        runner.configure("Kafka Connection", List.of(kafkaServerConfig, schemaRegistryConfig));
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

}
