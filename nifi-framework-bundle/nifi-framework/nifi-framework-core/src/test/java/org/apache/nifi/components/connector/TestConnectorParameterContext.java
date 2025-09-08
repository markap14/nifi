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

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConnectorParameterContext {

    @Mock
    private ConnectorNode mockConnectorNode;

    private ConnectorParameterContext parameterContext;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        parameterContext = new ConnectorParameterContext(mockConnectorNode);
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithEmptyParameters() {
        final Map<String, Parameter> parameters = Collections.emptyMap();
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameters, inheritedContexts);
        
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithNewParameter() {
        // Setup initial parameters (empty)
        parameterContext.setParameters(Collections.emptyMap());
        
        // Create a new parameter to add
        final Parameter newParameter = createParameter("param1", "value1", false, "Description 1");
        final Map<String, Parameter> parameterUpdates = Map.of("param1", newParameter);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertEquals(1, result.size());
        assertEquals(newParameter, result.get("param1"));
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithUpdatedParameter() {
        // Setup initial parameters
        final Parameter originalParameter = createParameter("param1", "originalValue", false, "Original Description");
        parameterContext.setParameters(Map.of("param1", originalParameter));
        
        // Create updated parameter
        final Parameter updatedParameter = createParameter("param1", "updatedValue", false, "Original Description");
        final Map<String, Parameter> parameterUpdates = Map.of("param1", updatedParameter);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertEquals(1, result.size());
        assertEquals(updatedParameter, result.get("param1"));
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithSensitivityChange() {
        // Setup initial parameters
        final Parameter originalParameter = createParameter("param1", "value1", false, "Description");
        parameterContext.setParameters(Map.of("param1", originalParameter));
        
        // Create parameter with changed sensitivity
        final Parameter updatedParameter = createParameter("param1", "value1", true, "Description");
        final Map<String, Parameter> parameterUpdates = Map.of("param1", updatedParameter);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertEquals(1, result.size());
        assertEquals(updatedParameter, result.get("param1"));
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithDescriptionChange() {
        // Setup initial parameters
        final Parameter originalParameter = createParameter("param1", "value1", false, "Original Description");
        parameterContext.setParameters(Map.of("param1", originalParameter));
        
        // Create parameter with changed description
        final Parameter updatedParameter = createParameter("param1", "value1", false, "Updated Description");
        final Map<String, Parameter> parameterUpdates = Map.of("param1", updatedParameter);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertEquals(1, result.size());
        assertEquals(updatedParameter, result.get("param1"));
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithRemovedParameter() {
        // Setup initial parameters
        final Parameter existingParameter = createParameter("param1", "value1", false, "Description");
        parameterContext.setParameters(Map.of("param1", existingParameter));
        
        // Remove parameter by setting it to null
        final Map<String, Parameter> parameterUpdates = new HashMap<>();
        parameterUpdates.put("param1", null);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertEquals(1, result.size());
        assertNull(result.get("param1"));
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithUnchangedParameter() {
        // Setup initial parameters
        final Parameter existingParameter = createParameter("param1", "value1", false, "Description");
        parameterContext.setParameters(Map.of("param1", existingParameter));
        
        // Submit the same parameter (no changes)
        final Parameter sameParameter = createParameter("param1", "value1", false, "Description");
        final Map<String, Parameter> parameterUpdates = Map.of("param1", sameParameter);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetEffectiveParameterUpdatesWithMixedOperations() {
        // Setup initial parameters
        final Parameter param1 = createParameter("param1", "value1", false, "Description 1");
        final Parameter param2 = createParameter("param2", "value2", false, "Description 2");
        final Parameter param3 = createParameter("param3", "value3", false, "Description 3");
        
        final Map<String, Parameter> initialParams = new HashMap<>();
        initialParams.put("param1", param1);
        initialParams.put("param2", param2);
        initialParams.put("param3", param3);
        parameterContext.setParameters(initialParams);
        
        // Mixed operations: add new, update existing, remove existing, keep unchanged
        final Parameter newParam4 = createParameter("param4", "value4", false, "Description 4");
        final Parameter updatedParam1 = createParameter("param1", "updatedValue1", false, "Description 1");
        
        final Map<String, Parameter> parameterUpdates = new HashMap<>();
        parameterUpdates.put("param1", updatedParam1);  // Update
        parameterUpdates.put("param2", null);           // Remove
        parameterUpdates.put("param4", newParam4);      // Add new
        // param3 not mentioned, so it should remain unchanged
        
        final List<ParameterContext> inheritedContexts = Collections.emptyList();
        
        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);
        
        assertEquals(3, result.size());
        assertEquals(updatedParam1, result.get("param1"));  // Updated
        assertNull(result.get("param2"));                   // Removed
        assertEquals(newParam4, result.get("param4"));      // Added
        // param3 should not be in result since it's unchanged
    }


    @Test
    public void testGetEffectiveParameterUpdatesWithNullDescriptions() {
        // Test handling of null descriptions
        final Parameter paramWithNullDesc = createParameter("param1", "value1", false, null);
        parameterContext.setParameters(Map.of("param1", paramWithNullDesc));

        final Parameter updatedParamWithNullDesc = createParameter("param1", "value1", false, null);
        final Map<String, Parameter> parameterUpdates = Map.of("param1", updatedParamWithNullDesc);
        final List<ParameterContext> inheritedContexts = Collections.emptyList();

        final Map<String, Parameter> result = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedContexts);

        // Should be empty since nothing actually changed
        assertTrue(result.isEmpty());
    }

    private Parameter createParameter(final String name, final String value, final boolean sensitive, final String description) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
            .name(name)
            .sensitive(sensitive)
            .description(description)
            .build();
        
        return new Parameter.Builder()
            .descriptor(descriptor)
            .value(value)
            .build();
    }
}
