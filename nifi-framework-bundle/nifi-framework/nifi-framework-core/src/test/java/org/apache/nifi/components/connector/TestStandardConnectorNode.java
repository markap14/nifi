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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardConnectorNode {

    private StandardConnectorNode connectorNode;
    private ScheduledExecutorService scheduler;

    @Mock
    private ExtensionManager extensionManager;
    
    @Mock
    private ProcessGroup managedProcessGroup;
    
    @Mock
    private BundleCoordinate bundleCoordinate;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        scheduler = new ScheduledThreadPoolExecutor(4);

        final SleepingConnector sleepingConnector = new SleepingConnector();
        connectorNode = new StandardConnectorNode(
            "test-connector-id",
            extensionManager,
            managedProcessGroup,
            createConnectorDetails(sleepingConnector),
            "TestConnector",
            bundleCoordinate
        );
    }

    private ConnectorDetails createConnectorDetails(final Connector connector) {
        final ComponentLog componentLog = new MockComponentLog("TestConnector", connector);
        return new ConnectorDetails(connector, bundleCoordinate, componentLog);
    }

    @Test
    public void testStartFromStoppedState() throws Exception {
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        
        final Future<Void> startFuture = connectorNode.start(scheduler);
        
        // Wait for start to complete
        startFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        assertEquals(ScheduledState.RUNNING, connectorNode.getDesiredState());
        assertTrue(startFuture.isDone());
        assertFalse(startFuture.isCancelled());
    }

    @Test
    public void testStopFromRunningState() throws Exception {
        // First start the connector
        final Future<Void> startFuture = connectorNode.start(scheduler);
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        
        // Now stop it
        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        stopFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ScheduledState.STOPPED, connectorNode.getDesiredState());
        assertTrue(stopFuture.isDone());
        assertFalse(stopFuture.isCancelled());
    }

    @Test
    public void testCannotStartFromDisabledState() {
        connectorNode.disable();
        assertEquals(ScheduledState.DISABLED, connectorNode.getCurrentState());
        
        assertThrows(IllegalStateException.class, () -> connectorNode.start(scheduler));
    }

    @Test
    public void testCannotTransitionFromDisabledToRunning() {
        connectorNode.disable();
        assertEquals(ScheduledState.DISABLED, connectorNode.getCurrentState());
        
        // Verify that starting throws an exception
        assertThrows(IllegalStateException.class, () -> connectorNode.start(scheduler));
        
        // State should remain disabled
        assertEquals(ScheduledState.DISABLED, connectorNode.getCurrentState());
    }

    @Test
    public void testEnableFromDisabledState() {
        connectorNode.disable();
        assertEquals(ScheduledState.DISABLED, connectorNode.getCurrentState());
        
        connectorNode.enable();
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        assertEquals(ScheduledState.STOPPED, connectorNode.getDesiredState());
    }

    @Test
    public void testDisableFromStoppedState() {
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        
        connectorNode.disable();
        assertEquals(ScheduledState.DISABLED, connectorNode.getCurrentState());
        assertEquals(ScheduledState.DISABLED, connectorNode.getDesiredState());
    }

    @Test
    public void testStartFutureCompletedOnlyWhenRunning() throws Exception {
        final Future<Void> startFuture = connectorNode.start(scheduler);
        
        // Future should complete when connector reaches RUNNING state
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        assertTrue(startFuture.isDone());
    }

    @Test
    public void testStopFutureCompletedOnlyWhenStopped() throws Exception {
        // Start first
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        
        // Then stop
        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        stopFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        assertTrue(stopFuture.isDone());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    public void testMultipleStartCallsReturnCompletedFutures() throws Exception {
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        
        final Future<Void> startFuture1 = connectorNode.start(scheduler);
        
        // Wait a bit then call start again while first is in progress
        Thread.sleep(50);
        final Future<Void> startFuture2 = connectorNode.start(scheduler);
        
        // Both futures should complete
        startFuture1.get(5, TimeUnit.SECONDS);
        startFuture2.get(5, TimeUnit.SECONDS);
        
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        assertTrue(startFuture1.isDone());
        assertTrue(startFuture2.isDone());
    }

    @Test
    public void testVerifyCanDeleteWhenStopped() {
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        assertDoesNotThrow(() -> connectorNode.verifyCanDelete());
    }

    @Test
    public void testVerifyCanDeleteWhenDisabled() {
        connectorNode.disable();
        assertEquals(ScheduledState.DISABLED, connectorNode.getCurrentState());
        assertDoesNotThrow(() -> connectorNode.verifyCanDelete());
    }

    @Test
    public void testCannotDeleteWhenRunning() throws Exception {
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        
        assertThrows(IllegalStateException.class, () -> connectorNode.verifyCanDelete());
    }

    @Test
    public void testVerifyCanStartWhenStopped() {
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        assertDoesNotThrow(() -> connectorNode.verifyCanStart());
    }

    @Test
    public void testStartAlreadyRunningReturnsImmediately() throws Exception {
        // Start the connector first
        connectorNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
        
        // Starting again should return immediately
        final Future<Void> startFuture = connectorNode.start(scheduler);
        assertTrue(startFuture.isDone());

        // Should complete very quickly since it's already running
        assertEquals(ScheduledState.RUNNING, connectorNode.getCurrentState());
    }

    @Test
    public void testStopAlreadyStoppedReturnsImmediately() {
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
        
        final Future<Void> stopFuture = connectorNode.stop(scheduler);
        assertTrue(stopFuture.isDone());

        // Should complete very quickly since it's already stopped
        assertEquals(ScheduledState.STOPPED, connectorNode.getCurrentState());
    }

    @Test
    public void testStartWhileStoppingQueuesStartFuture() throws Exception {
        // Create a slow-stopping connector to test the queuing behavior
        final SleepingConnector slowConnector = new SleepingConnector(Duration.ofMillis(300)); // 300ms stop delay
        final StandardConnectorNode slowNode = new StandardConnectorNode(
            "slow-connector-id",
            extensionManager,
            managedProcessGroup,
            createConnectorDetails(slowConnector),
            "SlowConnector",
            bundleCoordinate
        );
        
        // Start the slow connector
        slowNode.start(scheduler).get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, slowNode.getCurrentState());
        
        // Stop the connector (this will take time)
        final Future<Void> stopFuture = slowNode.stop(scheduler);
        
        // While stopping, try to start again - this should queue the start future
        assertEquals(ScheduledState.STOPPING, slowNode.getCurrentState());
        
        final Future<Void> startFuture = slowNode.start(scheduler);
        
        // Both futures should complete - stop first, then start
        stopFuture.get(5, TimeUnit.SECONDS);
        startFuture.get(5, TimeUnit.SECONDS);
        
        assertEquals(ScheduledState.RUNNING, slowNode.getCurrentState());
        assertTrue(stopFuture.isDone());
        assertTrue(startFuture.isDone());
    }

    @Test
    public void testCannotDeleteWhenStarting() throws Exception {
        // Use a slow-starting connector to test deletion during STARTING state
        final SleepingConnector slowConnector = new SleepingConnector(Duration.ofMillis(300));
        final StandardConnectorNode slowNode = new StandardConnectorNode(
            "slow-starting-connector-id",
            extensionManager,
            managedProcessGroup,
            createConnectorDetails(slowConnector),
            "SlowStartingConnector",
            bundleCoordinate
        );
        
        // Start the connector - this will take time
        final Future<Void> startFuture = slowNode.start(scheduler);
        
        // While starting, verify we cannot delete
        assertEquals(ScheduledState.STARTING, slowNode.getCurrentState());
        
        assertThrows(IllegalStateException.class, slowNode::verifyCanDelete);
        
        // Wait for start to complete
        startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(ScheduledState.RUNNING, slowNode.getCurrentState());
    }

}
