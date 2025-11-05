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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestStandardConnectorNode {

    private FlowEngine scheduler;

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private ProcessGroup managedProcessGroup;

    private FlowContextFactory flowContextFactory;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        scheduler = new FlowEngine(1, "flow-engine");

        flowContextFactory = new FlowContextFactory() {
            @Override
            public FrameworkFlowContext createActiveFlowContext(final ComponentLog connectorLogger) {
                final MutableConnectorConfigurationContext activeConfigurationContext = new StandardConnectorConfigurationContext();
                final ProcessGroupFacadeFactory processGroupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
                final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);
                return new StandardFlowContext(managedProcessGroup, activeConfigurationContext, processGroupFacadeFactory, parameterContextFacadeFactory, connectorLogger, FlowContextType.ACTIVE);
            }

            @Override
            public FrameworkFlowContext createWorkingFlowContext(final ComponentLog connectorLogger, final MutableConnectorConfigurationContext currentConfiguration) {
                final ProcessGroupFacadeFactory processGroupFacadeFactory = mock(ProcessGroupFacadeFactory.class);
                final ParameterContextFacadeFactory parameterContextFacadeFactory = mock(ParameterContextFacadeFactory.class);

                return new StandardFlowContext(managedProcessGroup, currentConfiguration, processGroupFacadeFactory, parameterContextFacadeFactory, connectorLogger, FlowContextType.WORKING);
            }
        };
    }

    @AfterEach
    public void teardown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    @Test
    public void testStartFromStoppedState() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> startFuture = connectorNode.start(scheduler);

        startFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());
        assertTrue(startFuture.isDone());
        assertFalse(startFuture.isCancelled());
    }

    @Test
    public void testStopFromRunningState() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        final Future<Void> startFuture = connectorNode.start(scheduler);
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        stopFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());
        assertTrue(stopFuture.isDone());
        assertFalse(stopFuture.isCancelled());
    }

    @Test
    public void testCannotStartFromDisabledState() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.disable();
        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());

        assertThrows(IllegalStateException.class, () -> connectorNode.start(scheduler));
    }

    @Test
    public void testCannotTransitionFromDisabledToRunning() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.disable();
        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());

        assertThrows(IllegalStateException.class, () -> connectorNode.start(scheduler));

        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());
    }

    @Test
    public void testEnableFromDisabledState() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.disable();
        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());

        connectorNode.enable();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());
    }

    @Test
    public void testDisableFromStoppedState() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.disable();
        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.DISABLED, connectorNode.getDesiredState());
    }

    @Test
    public void testStartFutureCompletedOnlyWhenRunning() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        final Future<Void> startFuture = connectorNode.start(scheduler);

        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertTrue(startFuture.isDone());
    }

    @Test
    public void testStopFutureCompletedOnlyWhenStopped() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        stopFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertTrue(stopFuture.isDone());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testMultipleStartCallsReturnCompletedFutures() throws Exception {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(startLatch, new CountDownLatch(0), new CountDownLatch(0));
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("BlockingConnectorNode");
        final StandardConnectorNode connectorNode = new StandardConnectorNode(
            "blocking-connector-id",
            extensionManager,
            null,
            managedProcessGroup,
            createConnectorDetails(blockingConnector),
            "BlockingConnector",
            null,
            new StandardConnectorConfigurationContext(),
            stateTransition,
            flowContextFactory
        );

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> startFuture1 = connectorNode.start(scheduler);
        assertEquals(ConnectorState.STARTING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        final Future<Void> startFuture2 = connectorNode.start(scheduler);
        assertEquals(ConnectorState.STARTING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        // Allow the connector to start
        startLatch.countDown();
        startFuture1.get(5, TimeUnit.SECONDS);
        startFuture2.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        assertTrue(startFuture1.isDone());
        assertTrue(startFuture2.isDone());
    }

    @Test
    public void testVerifyCanDeleteWhenStopped() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        connectorNode.verifyCanDelete();
    }

    @Test
    public void testVerifyCanDeleteWhenDisabled() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.disable();
        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());
        connectorNode.verifyCanDelete();
    }

    @Test
    public void testCannotDeleteWhenRunning() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        assertThrows(IllegalStateException.class, connectorNode::verifyCanDelete);
    }

    @Test
    public void testVerifyCanStartWhenStopped() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        connectorNode.verifyCanStart();
    }

    @Test
    public void testStartAlreadyRunningReturnsImmediately() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final Future<Void> startFuture = connectorNode.start(scheduler);
        assertTrue(startFuture.isDone());

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
    }

    @Test
    public void testStopAlreadyStoppedReturnsImmediately() {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        assertTrue(stopFuture.isDone());

        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    public void testStartWhileStoppingQueuesStartFuture() throws Exception {
        final CountDownLatch stopLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(new CountDownLatch(0), stopLatch, new CountDownLatch(0));
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("BlockingConnectorNode");
        final StandardConnectorNode connectorNode = new StandardConnectorNode(
            "blocking-connector-id",
            extensionManager,
            null,
            managedProcessGroup,
            createConnectorDetails(blockingConnector),
            "BlockingConnector",
            null,
            new StandardConnectorConfigurationContext(),
            stateTransition,
            flowContextFactory
        );

        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        assertEquals(ConnectorState.STOPPING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());

        final Future<Void> startFuture = connectorNode.start(scheduler);
        assertEquals(ConnectorState.STOPPING, connectorNode.getCurrentState());
        assertEquals(ConnectorState.RUNNING, connectorNode.getDesiredState());

        stopLatch.countDown();

        stopFuture.get(5, TimeUnit.SECONDS);
        startFuture.get(5, TimeUnit.SECONDS);

        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());
        assertTrue(stopFuture.isDone());
        assertTrue(startFuture.isDone());
    }

    @Test
    public void testCannotDeleteWhenStarting() throws Exception {
        // Use a slow-starting connector to test deletion during STARTING state
        final CountDownLatch startLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(startLatch, new CountDownLatch(0), new CountDownLatch(0));
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("SlowStartingConnectorNode");
        final StandardConnectorNode slowNode = new StandardConnectorNode(
            "slow-starting-connector-id",
            extensionManager,
            null,
            managedProcessGroup,
            createConnectorDetails(blockingConnector),
            "SlowStartingConnector",
            null,
            new StandardConnectorConfigurationContext(),
            stateTransition,
            flowContextFactory
        );

        // Start the connector - this will take time
        final Future<Void> startFuture = slowNode.start(scheduler);

        // While starting, verify we cannot delete
        assertEquals(ConnectorState.STARTING, slowNode.getCurrentState());

        assertThrows(IllegalStateException.class, slowNode::verifyCanDelete);

        // Wait for start to complete
        startLatch.countDown();
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, slowNode.getCurrentState());
    }

    @Test
    public void testSetConfigurationWhenStopped() throws FlowUpdateException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.STOPPED, connectorNode.getDesiredState());

        final ConnectorConfiguration newConfiguration = createTestConfiguration();

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("testGroup", createGroupConfig());
        connectorNode.finishUpdate(scheduler);

        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationWhenDisabled() throws FlowUpdateException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.disable();
        assertEquals(ConnectorState.DISABLED, connectorNode.getCurrentState());
        assertEquals(ConnectorState.DISABLED, connectorNode.getDesiredState());

        final ConnectorConfiguration newConfiguration = createTestConfiguration();

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("testGroup", createGroupConfig());
        connectorNode.finishUpdate(scheduler);

        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testCannotSetConfigurationWhenRunning() throws Exception {
        final StandardConnectorNode connectorNode = createConnectorNode();
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ConnectorState.RUNNING, connectorNode.getCurrentState());

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> connectorNode.setConfiguration("testGroup", createGroupConfig()));
        assertTrue(exception.getMessage().contains("state is currently RUNNING"));
    }

    @Test
    public void testCannotSetConfigurationWhenStarting() throws Exception {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(startLatch, new CountDownLatch(0), new CountDownLatch(0));
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("SlowStartingConnectorNode");
        final StandardConnectorNode slowNode = new StandardConnectorNode(
            "slow-starting-connector-id",
            extensionManager,
            null,
            managedProcessGroup,
            createConnectorDetails(blockingConnector),
            "SlowStartingConnector",
            null,
            new StandardConnectorConfigurationContext(),
            stateTransition,
            flowContextFactory
        );

        final Future<Void> startFuture = slowNode.start(scheduler);
        assertEquals(ConnectorState.STARTING, slowNode.getCurrentState());

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> slowNode.setConfiguration("testGroup", createGroupConfig()));
        assertTrue(exception.getMessage().contains("state is currently STARTING"));

        startLatch.countDown();
        startFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testCannotSetConfigurationWhenStopping() throws Exception {
        final CountDownLatch stopLatch = new CountDownLatch(1);
        final BlockingConnector blockingConnector = new BlockingConnector(new CountDownLatch(0), stopLatch, new CountDownLatch(0));
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("SlowStoppingConnectorNode");
        final StandardConnectorNode connectorNode = new StandardConnectorNode(
            "slow-stopping-connector-id",
            extensionManager,
            null,
            managedProcessGroup,
            createConnectorDetails(blockingConnector),
            "SlowStoppingConnector",
            null,
            new StandardConnectorConfigurationContext(),
            stateTransition,
            flowContextFactory
        );

        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        assertEquals(ConnectorState.STOPPING, connectorNode.getCurrentState());

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> connectorNode.setConfiguration("testGroup", createGroupConfig()));
        assertTrue(exception.getMessage().contains("state is currently STOPPING"));

        stopLatch.countDown();
        stopFuture.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSetConfigurationWithPropertyChanges() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("step1", createGroupConfig("propertyGroup1", Map.of("prop1", "value1")));
        connectorNode.finishUpdate(scheduler);

        final ConnectorConfiguration newConfiguration = createTestConfiguration("step1", "prop1", "value2");

        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("step1", createGroupConfig("propertyGroup1", Map.of("prop1", "value2")));
        connectorNode.finishUpdate(scheduler);
        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationWithNewConfigurationStep() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("configurationStep1", createGroupConfig("propertyGroup1", Map.of("prop1", "value1")));
        connectorNode.finishUpdate(scheduler);

        final ConnectorConfiguration newConfiguration = createTestConfigurationWithMultipleGroups();

        // Wait for Connector to fully stop
        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("configurationStep1", createGroupConfig("propertyGroup1", Map.of("prop1", "value1")));
        connectorNode.setConfiguration("configurationStep2", createGroupConfig("propertyGroup2", Map.of("prop2", "value2")));
        connectorNode.finishUpdate(scheduler);

        assertEquals(newConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationWithRemovedConfigurationStep() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final StandardConnectorNode connectorNode = createConnectorNode();
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("configurationStep1", createGroupConfig("propertyGroup1", Map.of("prop1", "value1")));
        connectorNode.setConfiguration("configurationStep2", createGroupConfig("propertyGroup2", Map.of("prop2", "value2")));
        connectorNode.finishUpdate(scheduler);

        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("configurationStep1", createGroupConfig("propertyGroup1", Map.of("prop1", "value1")));
        connectorNode.finishUpdate(scheduler);

        final List<ConfigurationStepConfiguration> expectedSteps = List.of(
            new ConfigurationStepConfiguration("configurationStep1", List.of(new PropertyGroupConfiguration("propertyGroup1", Map.of("prop1", "value1")))),
            new ConfigurationStepConfiguration("configurationStep2", List.of(new PropertyGroupConfiguration("propertyGroup2", Map.of("prop2", "value2"))))
        );
        final ConnectorConfiguration expectedConfiguration = new ConnectorConfiguration(expectedSteps);
        assertEquals(expectedConfiguration, connectorNode.getActiveFlowContext().getConfigurationContext().toConnectorConfiguration());
    }

    @Test
    public void testSetConfigurationCallsOnConfigured() throws FlowUpdateException {
        final TrackingConnector trackingConnector = new TrackingConnector();
        final StandardConnectorNode connectorNode = createConnectorNode(trackingConnector);
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("testGroup", createGroupConfig());
        connectorNode.finishUpdate(scheduler);
    }

    @Test
    public void testSetConfigurationCallsOnPropertyGroupConfiguredForChangedConfigurationSteps() throws FlowUpdateException, ExecutionException, InterruptedException, TimeoutException {
        final TrackingConnector trackingConnector = new TrackingConnector();
        final StandardConnectorNode connectorNode = createConnectorNode(trackingConnector);
        assertEquals(ConnectorState.STOPPED, connectorNode.getCurrentState());

        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("configurationStep1", createGroupConfig("configurationStep1", Map.of("prop1", "value1")));
        connectorNode.finishUpdate(scheduler);
        trackingConnector.reset();

        connectorNode.stop(scheduler).get(5, TimeUnit.SECONDS);
        connectorNode.prepareForUpdate(null);
        connectorNode.setConfiguration("configurationStep1", createGroupConfig("configurationStep1", Map.of("prop1", "value2")));
        connectorNode.finishUpdate(scheduler);

        assertTrue(trackingConnector.wasOnPropertyGroupConfiguredCalled("configurationStep1"));
    }

    private StandardConnectorNode createConnectorNode() {
        final SleepingConnector sleepingConnector = new SleepingConnector(Duration.ofMillis(1));
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("TestConnectorNode");

        return new StandardConnectorNode("test-connector-id", extensionManager, null, managedProcessGroup,
            createConnectorDetails(sleepingConnector), "TestConnector", null, new StandardConnectorConfigurationContext(),
            stateTransition, flowContextFactory);
    }

    private StandardConnectorNode createConnectorNode(final Connector connector) {
        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("TestConnectorNode");
        return new StandardConnectorNode("test-connector-id", extensionManager, null, managedProcessGroup,
            createConnectorDetails(connector), "TestConnector", null, new StandardConnectorConfigurationContext(),
            stateTransition, flowContextFactory);
    }

    private ConnectorDetails createConnectorDetails(final Connector connector) {
        final ComponentLog componentLog = new MockComponentLog("TestConnector", connector);
        return new ConnectorDetails(connector, null, componentLog);
    }

    private List<PropertyGroupConfiguration> createGroupConfig() {
        return createGroupConfig("propertyGroup1", Map.of("testProperty", "testValue"));
    }

    private List<PropertyGroupConfiguration> createGroupConfig(final String groupName, final Map<String, String> properties) {
        final PropertyGroupConfiguration propertyGroupConfiguration = new PropertyGroupConfiguration(groupName, properties);
        return List.of(propertyGroupConfiguration);
    }

    private ConnectorConfiguration createTestConfiguration() {
        return createTestConfiguration("testGroup", "testProperty", "testValue");
    }

    private ConnectorConfiguration createTestConfiguration(final String configurationStepName, final String propertyName, final String propertyValue) {
        final Map<String, String> properties = Map.of(propertyName, propertyValue);
        final PropertyGroupConfiguration propertyGroupConfiguration = new PropertyGroupConfiguration("propertyGroup1", properties);
        final ConfigurationStepConfiguration configurationStepConfiguration = new ConfigurationStepConfiguration(configurationStepName, List.of(propertyGroupConfiguration));
        return new ConnectorConfiguration(List.of(configurationStepConfiguration));
    }

    private ConnectorConfiguration createTestConfigurationWithMultipleGroups() {
        final Map<String, String> firstConfigurationStepProperties = Map.of("prop1", "value1");
        final PropertyGroupConfiguration firstPropertyGroupConfiguration = new PropertyGroupConfiguration("propertyGroup1", firstConfigurationStepProperties);
        final ConfigurationStepConfiguration firstConfigurationStepConfiguration = new ConfigurationStepConfiguration("configurationStep1", List.of(firstPropertyGroupConfiguration));

        final Map<String, String> secondConfigurationStepProperties = Map.of("prop2", "value2");
        final PropertyGroupConfiguration secondPropertyGroupConfiguration = new PropertyGroupConfiguration("propertyGroup2", secondConfigurationStepProperties);
        final ConfigurationStepConfiguration secondConfigurationStepConfiguration = new ConfigurationStepConfiguration("configurationStep2", List.of(secondPropertyGroupConfiguration));

        return new ConnectorConfiguration(List.of(firstConfigurationStepConfiguration, secondConfigurationStepConfiguration));
    }

    /**
     * Test connector that tracks method calls for verification
     */
    private static class TrackingConnector implements Connector {
        private final Set<String> onConfigurationStepConfiguredCalls = new HashSet<>();

        @Override
        public void initialize(final ConnectorInitializationContext connectorInitializationContext, final FlowContext flowContext) {
        }

        @Override
        public void start(final FlowContext flowContext) {
        }

        @Override
        public void stop(final FlowContext flowContext) {
        }

        @Override
        public List<ValidationResult> validate(final FlowContext flowContext, final ConnectorValidationContext connectorValidationContext) {
            return List.of();
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps(final FlowContext flowContext) {
            return List.of();
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public void abortUpdatePreparation(final FlowContext flowContext, final Throwable throwable) {
        }

        @Override
        public void finishUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String s, final Map<String, String> map, final FlowContext flowContext) {
            return List.of();
        }

        @Override
        public void onConfigurationStepConfigured(final String stepName, final FlowContext flowContext) {
            onConfigurationStepConfiguredCalls.add(stepName);
        }

        @Override
        public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName, final FlowContext workingContext) {
            return List.of();
        }

        @Override
        public List<AllowableValue> fetchAllowableValues(final String stepName, final String groupName, final String propertyName, final FlowContext workingContext, final String filter) {
            return List.of();
        }

        public boolean wasOnPropertyGroupConfiguredCalled(final String stepName) {
            return onConfigurationStepConfiguredCalls.contains(stepName);
        }

        public void reset() {
            onConfigurationStepConfiguredCalls.clear();
        }
    }

}
