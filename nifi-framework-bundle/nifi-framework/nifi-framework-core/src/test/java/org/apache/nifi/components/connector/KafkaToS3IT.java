/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.StandardValidationTrigger;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.StandardConnection;
import org.apache.nifi.connectors.kafkas3.KafkaConnectionStep;
import org.apache.nifi.connectors.kafkas3.KafkaToS3;
import org.apache.nifi.connectors.kafkas3.KafkaTopicsStep;
import org.apache.nifi.connectors.kafkas3.S3Step;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.GarbageCollectionLog;
import org.apache.nifi.controller.MockStateManagerProvider;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.StandardFlowManager;
import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.ContentRepositoryContext;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FileSystemRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.StandardContentRepositoryContext;
import org.apache.nifi.controller.repository.VolatileFlowFileRepository;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.scheduling.LifecycleStateManager;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.controller.scheduling.StandardLifecycleStateManager;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.scheduling.TimerDrivenSchedulingAgent;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.mock.MockNodeTypeProvider;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpackMode;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stateless.bootstrap.StatelessBootstrap;
import org.apache.nifi.stateless.queue.StatelessFlowFileQueue;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO: Delete this.
public class KafkaToS3IT {
    private static final BundleCoordinate BUNDLE_COORDINATE = new BundleCoordinate("org.apache.nifi", "nifi-kafka-to-s3-nar", "2.6.0-SNAPSHOT");

    private final FlowEngine flowEngine = new FlowEngine(4, "flow-engine");
    private StandardProcessScheduler processScheduler;
    private StandardFlowManager flowManager;
    private FlowEngine componentLifecycleThreadPool;
    private ConnectorRepository connectorRepository;
    private ExtensionDiscoveringManager extensionManager;
    private ConfluentKafkaContainer kafkaContainer;

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

    @BeforeEach
    public void setup() throws IOException, ClassNotFoundException {
        connectorRepository = new StandardConnectorRepository();

        extensionManager = new StandardExtensionDiscoveringManager();
        final BulletinRepository bulletinRepository = mock(BulletinRepository.class);
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();
        final LifecycleStateManager lifecycleStateManager = new StandardLifecycleStateManager();
        final ReloadComponent reloadComponent = mock(ReloadComponent.class);

        final FlowController flowController = mock(FlowController.class);
        when(flowController.isInitialized()).thenReturn(true);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(flowController.getReloadComponent()).thenReturn(reloadComponent);

        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties("src/test/resources/conf/nifi.properties");

        final ContentRepository contentRepo = new FileSystemRepository(nifiProperties);
        final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();
        final EventReporter eventReporter = mock(EventReporter.class);
        final ContentRepositoryContext contentRepoInitializationContext = new StandardContentRepositoryContext(resourceClaimManager, eventReporter);
        contentRepo.initialize(contentRepoInitializationContext);

        final FlowFileRepository flowFileRepo = new VolatileFlowFileRepository();
        flowFileRepo.initialize(resourceClaimManager);

        final ProvenanceRepository provRepo = new MockProvenanceRepository();
        final FlowFileEventRepository flowFileEventRepo = mock(FlowFileEventRepository.class);
        final CounterRepository counterRepo = mock(CounterRepository.class);
        final RepositoryContextFactory repoContextFactory = new RepositoryContextFactory(contentRepo, flowFileRepo, flowFileEventRepo, counterRepo, provRepo, stateManagerProvider, 1L);

        when(flowController.getRepositoryContextFactory()).thenReturn(repoContextFactory);
        when(flowController.getGarbageCollectionLog()).thenReturn(mock(GarbageCollectionLog.class));
        when(flowController.getProvenanceRepository()).thenReturn(provRepo);
        when(flowController.getBulletinRepository()).thenReturn(bulletinRepository);
        when(flowController.getLifecycleStateManager()).thenReturn(lifecycleStateManager);
        when(flowController.getFlowFileEventRepository()).thenReturn(mock(FlowFileEventRepository.class));
        when(flowController.getConnectorRepository()).thenReturn(connectorRepository);

        final ValidationTrigger validationTrigger = new StandardValidationTrigger(flowEngine, () -> true);
        when(flowController.getValidationTrigger()).thenReturn(validationTrigger);

        doAnswer(invocation -> {
            return createConnection(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4));
        }).when(flowController).createConnection(anyString(), nullable(String.class), any(Connectable.class), any(Connectable.class), anyCollection());

        final FlowFileEventRepository flowFileEventRepository = mock(FlowFileEventRepository.class);
        final ParameterContextManager parameterContextManager = mock(ParameterContextManager.class);

        final NodeTypeProvider nodeTypeProvider = new MockNodeTypeProvider();
        componentLifecycleThreadPool = new FlowEngine(4, "Component Lifecycle Thread Pool", true);
        processScheduler = new StandardProcessScheduler(componentLifecycleThreadPool, extensionManager, nodeTypeProvider, flowController::getControllerServiceProvider,
            reloadComponent, stateManagerProvider, nifiProperties, lifecycleStateManager);
        when(flowController.getProcessScheduler()).thenReturn(processScheduler);

        final FlowEngine flowEngine = new FlowEngine(10, "Timer-Driven Thread Pool");
        final SchedulingAgent timerDrivenSchedulingAgent = new TimerDrivenSchedulingAgent(flowController, flowEngine, repoContextFactory, nifiProperties);
        processScheduler.setSchedulingAgent(SchedulingStrategy.TIMER_DRIVEN, timerDrivenSchedulingAgent);

        final File assemblyLibDir = new File("../../../nifi-assembly/target/nifi-2.6.0-SNAPSHOT-bin/nifi-2.6.0-SNAPSHOT/lib");
        final ClassLoader systemClassLoader = StatelessBootstrap.createExtensionRootClassLoader(assemblyLibDir, ClassLoader.getSystemClassLoader());
        final String narLibraryDirectory = assemblyLibDir.getAbsolutePath();
        final Bundle systemBundle = SystemBundle.create(narLibraryDirectory, systemClassLoader);
        final File unpackDir = new File("target/unpacked-nars");
        final File frameworkWorkingDir = new File(unpackDir, "framework");
        final File extensionsWorkingDir = new File(unpackDir, "extensions");
        NarUnpacker.unpackNars(systemBundle, frameworkWorkingDir, extensionsWorkingDir, List.of(assemblyLibDir.toPath()), true,
            NarClassLoaders.FRAMEWORK_NAR_ID, true, false, NarUnpackMode.UNPACK_TO_UBER_JAR, bundleCoordinate -> true);


        flowManager = new StandardFlowManager(nifiProperties, null, flowController, flowFileEventRepository, parameterContextManager);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        final NarClassLoaders narClassLoaders = new NarClassLoaders();
        narClassLoaders.init(new File("target/unpacked-nars/framework"), new File("target/unpacked-nars/extensions"));
        final Set<Bundle> bundles = narClassLoaders.getBundles();
        extensionManager.discoverExtensions(systemBundle, bundles);
        Thread.currentThread().setContextClassLoader(systemClassLoader);

        final ControllerServiceProvider controllerServiceProvider = new StandardControllerServiceProvider(processScheduler, bulletinRepository, flowManager, extensionManager);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

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

        final ProcessGroup rootGroup = flowManager.createProcessGroup("root");
        rootGroup.setName("Root");
        flowManager.setRootGroup(rootGroup);
    }

    @AfterEach
    public void tearDown() {
        if (componentLifecycleThreadPool != null) {
            componentLifecycleThreadPool.shutdown();
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
    public void testCreate() {
        final ConnectorNode connectorNode = flowManager.createConnector(KafkaToS3.class.getName(), "kafka-to-s3", BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);
    }

    @Test
    public void testKafkaVerification() throws FlowUpdateException, IOException, InterruptedException {
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

        final ConnectorNode connectorNode = flowManager.createConnector(KafkaToS3.class.getName(), "kafka-to-s3", BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);

        connectorNode.prepareForUpdate(flowEngine);

        // Configure connection step
        final PropertyGroupConfiguration serverGroup = new PropertyGroupConfiguration(KafkaConnectionStep.KAFKA_SERVER_GROUP.getName(), Map.of(
            KafkaConnectionStep.KAFKA_BROKERS.getName(), "localhost:9093",
            KafkaConnectionStep.SECURITY_PROTOCOL.getName(), "SASL_PLAINTEXT",
            KafkaConnectionStep.SASL_MECHANISM.getName(), "PLAIN",
            KafkaConnectionStep.USERNAME.getName(), SCRAM_USERNAME,
            KafkaConnectionStep.PASSWORD.getName(), SCRAM_PASSWORD
        ));

        // Ensure that the configuration will be valid
        final List<ConfigVerificationResult> connectionValidationResults = connectorNode.verifyConfigurationStep(KafkaConnectionStep.STEP_NAME, List.of(serverGroup));
        assertEquals(List.of(), connectionValidationResults);

        // Set the configuration on the Connector.
        connectorNode.setConfiguration(KafkaConnectionStep.STEP_NAME, List.of(serverGroup));

        // Get the updated list of Configuration Steps. This should now include a list of available topics as Allowable Values for the "Topic Names" property.
        final List<ConfigurationStep> configurationSteps = connectorNode.getConfigurationSteps();
        assertEquals(3, configurationSteps.size());

        final ConfigurationStep topicsStep = configurationSteps.get(1);
        final ConnectorPropertyGroup topicsGroup = topicsStep.getPropertyGroups().getFirst();
        final List<ConnectorPropertyDescriptor> propertyDescriptors = topicsGroup.getProperties();
        final ConnectorPropertyDescriptor topicNamesDescriptor = propertyDescriptors.getFirst();
        assertEquals("Topic Names", topicNamesDescriptor.getName());

        final List<DescribedValue> allowedTopicValues = topicNamesDescriptor.getAllowableValues();
        assertNull(allowedTopicValues);
        assertTrue(topicNamesDescriptor.isAllowableValuesFetchable());

        final List<AllowableValue> fetchedAllowedTopicValues = connectorNode.fetchAllowableValues(KafkaTopicsStep.STEP_NAME, topicsGroup.getName(), topicNamesDescriptor.getName());
        assertNotNull(fetchedAllowedTopicValues);
        final List<String> topicNames = fetchedAllowedTopicValues.stream().map(DescribedValue::getValue).toList();
        assertEquals(List.of("an-important-topic", "topic-1", "topic-2", "topic-3", "topic-4", "topic-5", "Z-topic"), topicNames);

        // Create configuration to point to "topic-1" topic.
        final PropertyGroupConfiguration topic1GroupConfig = new PropertyGroupConfiguration(topicsGroup.getName(), Map.of(
            KafkaTopicsStep.TOPIC_NAMES.getName(), "topic-1",
            KafkaTopicsStep.CONSUMER_GROUP_ID.getName(), "kafka-to-s3-test-group",
            KafkaTopicsStep.OFFSET_RESET.getName(), "earliest",
            KafkaTopicsStep.KAFKA_DATA_FORMAT.getName(), "JSON"
        ));

        // Validate the configuration for the topics step. This is expected to be valid.
        final List<ConfigVerificationResult> topic1ValidationResults = connectorNode.verifyConfigurationStep(KafkaTopicsStep.STEP_NAME, List.of(topic1GroupConfig));
        assertEquals(List.of(), topic1ValidationResults.stream().filter(result -> result.getOutcome() == Outcome.FAILED).toList());

        // Create configuration to point to "an-important-topic" topic.
        final PropertyGroupConfiguration importantTopicGroupConfig = new PropertyGroupConfiguration(topicsGroup.getName(), Map.of(
            KafkaTopicsStep.TOPIC_NAMES.getName(), "an-important-topic",
            KafkaTopicsStep.CONSUMER_GROUP_ID.getName(), "kafka-to-s3-test-group",
            KafkaTopicsStep.OFFSET_RESET.getName(), "earliest",
            KafkaTopicsStep.KAFKA_DATA_FORMAT.getName(), "JSON"
        ));

        // Validate the configuration for the topics step. We expect 1 validation issue because the data format is set to JSON but the topic contains plaintext messages.
        final List<ConfigVerificationResult> importantTopicValidationResults = connectorNode.verifyConfigurationStep(KafkaTopicsStep.STEP_NAME, List.of(importantTopicGroupConfig));
        final List<ConfigVerificationResult> invalidImportantTopicResults = importantTopicValidationResults.stream()
            .filter(result -> result.getOutcome() == Outcome.FAILED)
            .toList();
        assertEquals(1, invalidImportantTopicResults.size());
        final ConfigVerificationResult invalidResult = invalidImportantTopicResults.getFirst();
        assertTrue(invalidResult.getExplanation().contains("parse"), "Unexpected validation reason: " + invalidResult.getExplanation());

        connectorNode.finishUpdate(flowEngine);
    }

    @Test
    public void testFullFlow() throws IOException, InterruptedException, FlowUpdateException, ExecutionException, TimeoutException {
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

        final ConnectorNode connectorNode = flowManager.createConnector(KafkaToS3.class.getName(), "kafka-to-s3", BUNDLE_COORDINATE, true, true);
        assertNotNull(connectorNode);

        final PropertyGroupConfiguration serverGroup = new PropertyGroupConfiguration(KafkaConnectionStep.KAFKA_SERVER_GROUP.getName(), Map.of(
            KafkaConnectionStep.KAFKA_BROKERS.getName(), "localhost:9093",
            KafkaConnectionStep.SECURITY_PROTOCOL.getName(), "SASL_PLAINTEXT",
            KafkaConnectionStep.SASL_MECHANISM.getName(), "PLAIN",
            KafkaConnectionStep.USERNAME.getName(), SCRAM_USERNAME,
            KafkaConnectionStep.PASSWORD.getName(), SCRAM_PASSWORD
        ));

        final PropertyGroupConfiguration topicGroupConfig = new PropertyGroupConfiguration(KafkaTopicsStep.KAFKA_TOPICS_GROUP.getName(), Map.of(
            KafkaTopicsStep.TOPIC_NAMES.getName(), "story",
            KafkaTopicsStep.CONSUMER_GROUP_ID.getName(), "kafka-to-s3-test-group-testFullFlow",
            KafkaTopicsStep.OFFSET_RESET.getName(), "earliest",
            KafkaTopicsStep.KAFKA_DATA_FORMAT.getName(), "JSON"
        ));

        final PropertyGroupConfiguration s3DestinationGroup = new PropertyGroupConfiguration(S3Step.S3_DESTINATION_GROUP.getName(), Map.of(
            S3Step.S3_REGION.getName(), "us-west-2",
            S3Step.S3_DATA_FORMAT.getName(), "Avro",
            S3Step.S3_BUCKET.getName(), "mpayne-test-bucket-123"
        ));
        final PropertyGroupConfiguration s3AuthGroup = new PropertyGroupConfiguration(S3Step.S3_DESTINATION_GROUP.getName(), Map.of(
            S3Step.S3_AUTHENTICATION_STRATEGY.getName(), S3Step.DEFAULT_CREDENTIALS
        ));
        final PropertyGroupConfiguration mergeGroup = new PropertyGroupConfiguration(S3Step.MERGE_GROUP.getName(), Map.of(
            S3Step.TARGET_OBJECT_SIZE.getName(), "1 MB",
            S3Step.MERGE_LATENCY.getName(), "5 sec"
        ));

        connectorNode.prepareForUpdate(flowEngine);
        connectorNode.setConfiguration(KafkaConnectionStep.STEP_NAME, List.of(serverGroup));
        connectorNode.setConfiguration(KafkaTopicsStep.STEP_NAME, List.of(topicGroupConfig));
        connectorNode.setConfiguration(S3Step.S3_STEP.getName(), List.of(s3DestinationGroup, s3AuthGroup, mergeGroup));
        connectorNode.finishUpdate(flowEngine);

        final ValidationState validationState = connectorNode.performValidation();
        assertEquals(ValidationStatus.VALID, validationState.getStatus(), "Connector is invalid due to: " + validationState.getValidationErrors());

        connectorNode.start(flowEngine).get(1, TimeUnit.MINUTES);

        // wait for some data to be received
        while (connectorNode.getFlowFileTransferCounts().getReceivedCount() == 0) {
            Thread.sleep(100L);
        }

        // Wait for Connector to be idle for at least 1 second
        while (connectorNode.getIdleDuration().orElse(Duration.ZERO).toSeconds() < 1) {
            Thread.sleep(100L);
        }

        connectorNode.stop(flowEngine).get(1, TimeUnit.MINUTES);

        assertEquals(1, connectorNode.getFlowFileTransferCounts().getSentCount());
    }

    private Connection createConnection(final String id, final String name, final Connectable source, final Connectable destination, final Collection<String> relationshipNames) {
        final List<Relationship> relationships = relationshipNames.stream()
            .map(relName -> new Relationship.Builder().name(relName).build())
            .toList();

        final FlowFileQueueFactory flowFileQueueFactory = (loadBalanceStrategy, partitioningAttribute, processGroup) -> new StatelessFlowFileQueue(UUID.randomUUID().toString());
        final Connection connection = new StandardConnection.Builder(processScheduler)
            .id(id)
            .name(name)
            .processGroup(destination.getProcessGroup())
            .relationships(relationships)
            .source(requireNonNull(source))
            .destination(destination)
            .flowFileQueueFactory(flowFileQueueFactory)
            .build();

        return connection;
    }

}
