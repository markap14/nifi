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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardConnectorConfigurationContext implements ConnectorConfigurationContext {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    final Map<String, List<PropertyGroupConfiguration>> propertyGroupConfigurations = new HashMap<>();


    @Override
    public ConnectorPropertyValue getProperty(final String stepName, final String propertyName) {
        return getProperty(stepName, propertyName, null);
    }

    private ConnectorPropertyValue getProperty(final String stepName, final String propertyName, final String defaultValue) {
        readLock.lock();
        try {
            final List<PropertyGroupConfiguration> groupConfigs = propertyGroupConfigurations.get(stepName);
            if (groupConfigs == null) {
                return new StandardConnectorPropertyValue(defaultValue);
            }

            String propertyValue = defaultValue;
            for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
                final String value = groupConfig.getPropertyValues().get(propertyName);
                if (value != null) {
                    propertyValue = value;
                    break;
                }
            }

            return new StandardConnectorPropertyValue(propertyValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ConnectorPropertyValue getProperty(final ConfigurationStep configurationStep, final ConnectorPropertyDescriptor connectorPropertyDescriptor) {
        return getProperty(configurationStep.getName(), connectorPropertyDescriptor.getName(), connectorPropertyDescriptor.getDefaultValue());
    }

    @Override
    public StandardConnectorConfigurationContext createWithOverrides(final String stepName, final Map<String, String> propertyOverrides) {
        final StandardConnectorConfigurationContext created = new StandardConnectorConfigurationContext();
        readLock.lock();
        try {
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : propertyGroupConfigurations.entrySet()) {
                final String existingStepName = entry.getKey();
                final List<PropertyGroupConfiguration> createdGroupConfigs = new ArrayList<>();

                for (final PropertyGroupConfiguration groupConfig : entry.getValue()) {
                    final Map<String, String> mergedProperties = new HashMap<>(groupConfig.getPropertyValues());
                    if (Objects.equals(existingStepName, stepName)) {
                        for (final Map.Entry<String, String> overrideEntry : propertyOverrides.entrySet()) {
                            if (mergedProperties.containsKey(overrideEntry.getKey())) {
                                mergedProperties.put(overrideEntry.getKey(), overrideEntry.getValue());
                            }
                        }
                    }

                    createdGroupConfigs.add(new PropertyGroupConfiguration(groupConfig.getPropertyGroupName(), new HashMap<>(groupConfig.getPropertyValues())));
                }

                created.setProperties(existingStepName, createdGroupConfigs);
            }

            return created;
        } finally {
            readLock.unlock();
        }
    }

    ConfigurationUpdateResult setProperties(final String stepName, final List<PropertyGroupConfiguration> propertyGroupConfigurations) {
        writeLock.lock();
        try {
            final List<PropertyGroupConfiguration> existingGroupConfigs = this.propertyGroupConfigurations.get(stepName);
            if (Objects.equals(existingGroupConfigs, propertyGroupConfigurations)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            this.propertyGroupConfigurations.put(stepName, merge(existingGroupConfigs, propertyGroupConfigurations));
            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    ConnectorConfiguration toConnectorConfiguration() {
        readLock.lock();
        try {
            final List<ConfigurationStepConfiguration> stepConfigs = new ArrayList<>();
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : propertyGroupConfigurations.entrySet()) {
                final String stepName = entry.getKey();
                final List<PropertyGroupConfiguration> groupConfigurations = entry.getValue();

                stepConfigs.add(new ConfigurationStepConfiguration(stepName, groupConfigurations));
            }

            return new ConnectorConfiguration(stepConfigs);
        } finally {
            readLock.unlock();
        }
    }

    private List<PropertyGroupConfiguration> merge(final List<PropertyGroupConfiguration> existingGroupConfigs, final List<PropertyGroupConfiguration> newGroupConfigs) {
        if (existingGroupConfigs == null || existingGroupConfigs.isEmpty()) {
            return new ArrayList<>(newGroupConfigs);
        }

        if (newGroupConfigs == null || newGroupConfigs.isEmpty()) {
            return existingGroupConfigs;
        }

        final Map<String, PropertyGroupConfiguration> mergedMap = new HashMap<>();
        for (final PropertyGroupConfiguration groupConfig : existingGroupConfigs) {
            mergedMap.put(groupConfig.getPropertyGroupName(), groupConfig);
        }

        for (final PropertyGroupConfiguration groupConfig : newGroupConfigs) {
            final PropertyGroupConfiguration existingConfiguration = mergedMap.get(groupConfig.getPropertyGroupName());
            final PropertyGroupConfiguration mergedGroupConfig = merge(existingConfiguration, groupConfig);
            mergedMap.put(groupConfig.getPropertyGroupName(), mergedGroupConfig);
        }

        return List.copyOf(mergedMap.values());
    }

    private PropertyGroupConfiguration merge(final PropertyGroupConfiguration existingConfiguration, final PropertyGroupConfiguration newConfiguration) {
        if (Objects.equals(existingConfiguration, newConfiguration)) {
            return existingConfiguration;
        }
        if (existingConfiguration == null) {
            return newConfiguration;
        }
        if (newConfiguration == null) {
            return existingConfiguration;
        }

        final Map<String, String> mergedProperties = new HashMap<>(existingConfiguration.getPropertyValues());
        mergedProperties.putAll(newConfiguration.getPropertyValues());
        return new PropertyGroupConfiguration(existingConfiguration.getPropertyGroupName(), mergedProperties);
    }
}
