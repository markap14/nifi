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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestStandardConnectorConfigurationContext {
    private StandardConnectorConfigurationContext context;

    @BeforeEach
    public void setUp() {
        context = new StandardConnectorConfigurationContext();
    }

    @Test
    public void testSetPropertiesWithNoExistingConfigurations() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        final PropertyGroupConfiguration group = new PropertyGroupConfiguration("group1", properties);
        context.setProperties("step1", List.of(group));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value2", context.getProperty("step1", "key2").getValue());
    }

    @Test
    public void testSetPropertiesAddsNewPropertyGroup() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        final PropertyGroupConfiguration initialGroup = new PropertyGroupConfiguration("group1", initialProperties);
        context.setProperties("step1", List.of(initialGroup));

        final Map<String, String> newProperties = new HashMap<>();
        newProperties.put("key3", "value3");
        final PropertyGroupConfiguration newGroup = new PropertyGroupConfiguration("group2", newProperties);
        context.setProperties("step1", List.of(newGroup));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesMergesExistingPropertyGroupWithNewValues() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        final PropertyGroupConfiguration initialGroup = new PropertyGroupConfiguration("group1", initialProperties);
        context.setProperties("step1", List.of(initialGroup));

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", "updatedValue2");
        updatedProperties.put("key3", "value3");
        final PropertyGroupConfiguration updatedGroup = new PropertyGroupConfiguration("group1", updatedProperties);
        context.setProperties("step1", List.of(updatedGroup));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesRemovesKeyWhenValueIsNull() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        initialProperties.put("key3", "value3");
        final PropertyGroupConfiguration initialGroup = new PropertyGroupConfiguration("group1", initialProperties);
        context.setProperties("step1", List.of(initialGroup));

        assertEquals("value2", context.getProperty("step1", "key2").getValue());

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", null);
        final PropertyGroupConfiguration updatedGroup = new PropertyGroupConfiguration("group1", updatedProperties);
        context.setProperties("step1", List.of(updatedGroup));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertNull(context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesLeavesUnprovidedKeysAsIs() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        initialProperties.put("key3", "value3");
        final PropertyGroupConfiguration initialGroup = new PropertyGroupConfiguration("group1", initialProperties);
        context.setProperties("step1", List.of(initialGroup));

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", "updatedValue2");
        final PropertyGroupConfiguration updatedGroup = new PropertyGroupConfiguration("group1", updatedProperties);
        context.setProperties("step1", List.of(updatedGroup));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesPreservesUnmodifiedGroups() {
        final Map<String, String> group1Properties = new HashMap<>();
        group1Properties.put("key1", "value1");
        final PropertyGroupConfiguration group1 = new PropertyGroupConfiguration("group1", group1Properties);

        final Map<String, String> group2Properties = new HashMap<>();
        group2Properties.put("key2", "value2");
        final PropertyGroupConfiguration group2 = new PropertyGroupConfiguration("group2", group2Properties);

        context.setProperties("step1", List.of(group1, group2));

        final Map<String, String> group3Properties = new HashMap<>();
        group3Properties.put("key3", "value3");
        final PropertyGroupConfiguration group3 = new PropertyGroupConfiguration("group3", group3Properties);

        context.setProperties("step1", List.of(group3));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesWithMultipleGroupsMergingAndAdding() {
        final Map<String, String> group1Properties = new HashMap<>();
        group1Properties.put("key1", "value1");
        group1Properties.put("key2", "value2");
        final PropertyGroupConfiguration group1 = new PropertyGroupConfiguration("group1", group1Properties);

        final Map<String, String> group2Properties = new HashMap<>();
        group2Properties.put("key3", "value3");
        final PropertyGroupConfiguration group2 = new PropertyGroupConfiguration("group2", group2Properties);

        context.setProperties("step1", List.of(group1, group2));

        final Map<String, String> updatedGroup1Properties = new HashMap<>();
        updatedGroup1Properties.put("key2", "updatedValue2");
        updatedGroup1Properties.put("key4", "value4");
        final PropertyGroupConfiguration updatedGroup1 = new PropertyGroupConfiguration("group1", updatedGroup1Properties);

        final Map<String, String> group3Properties = new HashMap<>();
        group3Properties.put("key5", "value5");
        final PropertyGroupConfiguration group3 = new PropertyGroupConfiguration("group3", group3Properties);

        context.setProperties("step1", List.of(updatedGroup1, group3));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
        assertEquals("value4", context.getProperty("step1", "key4").getValue());
        assertEquals("value5", context.getProperty("step1", "key5").getValue());
    }

    @Test
    public void testSetPropertiesForDifferentSteps() {
        final Map<String, String> step1Properties = new HashMap<>();
        step1Properties.put("key1", "value1");
        final PropertyGroupConfiguration step1Group = new PropertyGroupConfiguration("group1", step1Properties);
        context.setProperties("step1", List.of(step1Group));

        final Map<String, String> step2Properties = new HashMap<>();
        step2Properties.put("key2", "value2");
        final PropertyGroupConfiguration step2Group = new PropertyGroupConfiguration("group1", step2Properties);
        context.setProperties("step2", List.of(step2Group));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertNull(context.getProperty("step1", "key2").getValue());
        assertEquals("value2", context.getProperty("step2", "key2").getValue());
        assertNull(context.getProperty("step2", "key1").getValue());
    }

    @Test
    public void testGetPropertyReturnsEmptyForNonExistentStep() {
        final ConnectorPropertyValue propertyValue = context.getProperty("nonExistentStep", "someProperty");
        assertNull(propertyValue.getValue());
    }

    @Test
    public void testGetPropertyReturnsEmptyForNonExistentProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        final PropertyGroupConfiguration group = new PropertyGroupConfiguration("group1", properties);
        context.setProperties("step1", List.of(group));

        final ConnectorPropertyValue propertyValue = context.getProperty("step1", "nonExistentProperty");
        assertNull(propertyValue.getValue());
    }

    @Test
    public void testComplexMergingScenario() {
        final Map<String, String> group1InitialProps = new HashMap<>();
        group1InitialProps.put("a", "1");
        group1InitialProps.put("b", "2");
        group1InitialProps.put("c", "3");
        final PropertyGroupConfiguration group1Initial = new PropertyGroupConfiguration("group1", group1InitialProps);

        final Map<String, String> group2InitialProps = new HashMap<>();
        group2InitialProps.put("d", "4");
        group2InitialProps.put("e", "5");
        final PropertyGroupConfiguration group2Initial = new PropertyGroupConfiguration("group2", group2InitialProps);

        final Map<String, String> group3InitialProps = new HashMap<>();
        group3InitialProps.put("f", "6");
        final PropertyGroupConfiguration group3Initial = new PropertyGroupConfiguration("group3", group3InitialProps);

        context.setProperties("step1", List.of(group1Initial, group2Initial, group3Initial));

        final Map<String, String> group1UpdateProps = new HashMap<>();
        group1UpdateProps.put("b", null);
        group1UpdateProps.put("c", "30");
        group1UpdateProps.put("g", "7");
        final PropertyGroupConfiguration group1Update = new PropertyGroupConfiguration("group1", group1UpdateProps);

        final Map<String, String> group4Props = new HashMap<>();
        group4Props.put("h", "8");
        final PropertyGroupConfiguration group4 = new PropertyGroupConfiguration("group4", group4Props);

        context.setProperties("step1", List.of(group1Update, group4));

        assertEquals("1", context.getProperty("step1", "a").getValue());
        assertNull(context.getProperty("step1", "b").getValue());
        assertEquals("30", context.getProperty("step1", "c").getValue());
        assertEquals("4", context.getProperty("step1", "d").getValue());
        assertEquals("5", context.getProperty("step1", "e").getValue());
        assertEquals("6", context.getProperty("step1", "f").getValue());
        assertEquals("7", context.getProperty("step1", "g").getValue());
        assertEquals("8", context.getProperty("step1", "h").getValue());
    }
}

