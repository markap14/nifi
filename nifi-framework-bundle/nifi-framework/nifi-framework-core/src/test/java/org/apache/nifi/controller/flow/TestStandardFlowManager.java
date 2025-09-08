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

package org.apache.nifi.controller.flow;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardFlowManager {

    private StandardFlowManager flowManager;

    @Mock
    private NiFiProperties nifiProperties;

    @Mock
    private SSLContext sslContext;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowFileEventRepository flowFileEventRepository;

    @Mock
    private ParameterContextManager parameterContextManager;

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private ProcessGroup managedProcessGroup;

    @Mock
    private BundleCoordinate bundleCoordinate;

    @Mock
    private Bundle bundle;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(flowController.isInitialized()).thenReturn(true);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(extensionManager.getBundle(bundleCoordinate)).thenReturn(bundle);
        when(bundle.getClassLoader()).thenReturn(NopConnector.class.getClassLoader());

        flowManager = new StandardFlowManager(nifiProperties, sslContext, flowController, flowFileEventRepository, parameterContextManager);
    }

    @Test
    public void testCreateConnectorParameterValidation() {
        final String validConnectorType = NopConnector.class.getName();
        final String validConnectorId = "test-connector-123";

        final NullPointerException typeException = assertThrows(NullPointerException.class, () -> {
            flowManager.createConnector(null, validConnectorId, bundleCoordinate, true, true);
        });
        assertEquals("Connector Type", typeException.getMessage());

        final NullPointerException idException = assertThrows(NullPointerException.class, () -> {
            flowManager.createConnector(validConnectorType, null, bundleCoordinate, true, true);
        });
        assertEquals("Connector ID", idException.getMessage());

        final NullPointerException bundleException = assertThrows(NullPointerException.class, () -> {
            flowManager.createConnector(validConnectorType, validConnectorId, null, true, true);
        });
        assertEquals("Bundle Coordinate", bundleException.getMessage());
    }

    @Test
    public void testConnectorRegistryOperations() {
        final ConnectorNode connector1 = createMockConnectorNode("connector-1");
        final ConnectorNode connector2 = createMockConnectorNode("connector-2");

        flowManager.onConnectorAdded(connector1);
        assertEquals(1, flowManager.getAllConnectors().size());
        assertTrue(flowManager.getAllConnectors().contains(connector1));

        flowManager.onConnectorAdded(connector2);
        assertEquals(2, flowManager.getAllConnectors().size());
        assertTrue(flowManager.getAllConnectors().contains(connector1));
        assertTrue(flowManager.getAllConnectors().contains(connector2));

        flowManager.onConnectorRemoved(connector1);
        assertEquals(1, flowManager.getAllConnectors().size());
        assertFalse(flowManager.getAllConnectors().contains(connector1));
        assertTrue(flowManager.getAllConnectors().contains(connector2));

        flowManager.onConnectorRemoved(connector2);
        assertEquals(0, flowManager.getAllConnectors().size());
    }

    @Test
    public void testConnectorRegistryNullHandling() {
        flowManager.onConnectorAdded(null);
        assertEquals(0, flowManager.getAllConnectors().size());

        flowManager.onConnectorRemoved(null);
        assertEquals(0, flowManager.getAllConnectors().size());
    }

    @Test
    public void testGetAllConnectorsReturnsNewListInstances() {
        final ConnectorNode connector1 = createMockConnectorNode("connector-1");
        final ConnectorNode connector2 = createMockConnectorNode("connector-2");

        flowManager.onConnectorAdded(connector1);
        flowManager.onConnectorAdded(connector2);

        final List<ConnectorNode> allConnectors1 = flowManager.getAllConnectors();
        final List<ConnectorNode> allConnectors2 = flowManager.getAllConnectors();

        assertEquals(2, allConnectors1.size());
        assertEquals(2, allConnectors2.size());
        assertEquals(allConnectors1, allConnectors2);
        assertTrue(allConnectors1 != allConnectors2); // Different object references
    }

    @Test
    public void testConnectorRegistryComplexOperations() {
        final ConnectorNode connector1 = createMockConnectorNode("connector-1");
        final ConnectorNode connector2 = createMockConnectorNode("connector-2");
        final ConnectorNode connector3 = createMockConnectorNode("connector-3");
        
        flowManager.onConnectorAdded(connector1);
        flowManager.onConnectorAdded(connector2);
        flowManager.onConnectorAdded(connector3);
        assertEquals(3, flowManager.getAllConnectors().size());

        flowManager.onConnectorRemoved(connector2);
        final List<ConnectorNode> connectorsAfterRemoval = flowManager.getAllConnectors();
        assertEquals(2, connectorsAfterRemoval.size());
        assertTrue(connectorsAfterRemoval.contains(connector1));
        assertFalse(connectorsAfterRemoval.contains(connector2));
        assertTrue(connectorsAfterRemoval.contains(connector3));

        flowManager.onConnectorRemoved(connector1);
        flowManager.onConnectorRemoved(connector3);
        assertEquals(0, flowManager.getAllConnectors().size());
    }

    @Test
    public void testConnectorIdUniquenessHandling() {
        final ConnectorNode connector1 = createMockConnectorNode("same-connector-id");
        final ConnectorNode connector2 = createMockConnectorNode("same-connector-id");

        flowManager.onConnectorAdded(connector1);
        assertEquals(1, flowManager.getAllConnectors().size());

        flowManager.onConnectorAdded(connector2);
        assertEquals(List.of(connector2), flowManager.getAllConnectors());
    }

    private ConnectorNode createMockConnectorNode(final String identifier) {
        final ConnectorNode connectorNode = mock(ConnectorNode.class);
        when(connectorNode.getIdentifier()).thenReturn(identifier);
        return connectorNode;
    }
}
