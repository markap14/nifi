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

public class StandardConnectorConfigurationContext implements MutableConnectorConfigurationContext, Cloneable {
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    final Map<String, List<PropertyGroupConfiguration>> propertyGroupConfigurations = new HashMap<>();

    public StandardConnectorConfigurationContext() {
    }

    @Override
    public ConnectorPropertyValue getProperty(final String stepName, final String groupName, final String propertyName) {
        return getProperty(stepName, groupName, propertyName, null);
    }

    private ConnectorPropertyValue getProperty(final String stepName, final String groupName, final String propertyName, final String defaultValue) {
        readLock.lock();
        try {
            final List<PropertyGroupConfiguration> groupConfigs = propertyGroupConfigurations.get(stepName);
            if (groupConfigs == null) {
                return new StandardConnectorPropertyValue(defaultValue);
            }

            for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
                if (!Objects.equals(groupConfig.groupName(), groupName)) {
                    continue;
                }

                final ConnectorValueReference valueReference = groupConfig.propertyValues().get(propertyName);
                if (valueReference == null) {
                    return new StandardConnectorPropertyValue(defaultValue);
                }

                return new StandardConnectorPropertyValue(valueReference.value());
            }

            // Property Group not found
            return new StandardConnectorPropertyValue(defaultValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ConnectorPropertyValue getProperty(final ConfigurationStep configurationStep, final ConnectorPropertyGroup group, final ConnectorPropertyDescriptor connectorPropertyDescriptor) {
        return getProperty(configurationStep.getName(), group.getName(), connectorPropertyDescriptor.getName(), connectorPropertyDescriptor.getDefaultValue());
    }

    @Override
    public StandardConnectorConfigurationContext createWithOverrides(final String stepName, final List<PropertyGroupConfiguration> propertyOverrides) {
        final StandardConnectorConfigurationContext created = new StandardConnectorConfigurationContext();
        readLock.lock();
        try {
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> stepEntry : propertyGroupConfigurations.entrySet()) {
                final String existingStepName = stepEntry.getKey();
                final List<PropertyGroupConfiguration> existingGroupConfigs = stepEntry.getValue();

                // If this is not the step to override, just copy the existing configs.
                if (!existingStepName.equals(stepName)) {
                    created.setProperties(existingStepName, existingGroupConfigs);
                    continue;
                }

                final List<PropertyGroupConfiguration> createdGroupConfigs = new ArrayList<>();

                // Merge properties for this step.
                final Map<String, PropertyGroupConfiguration> existingGroupConfigMap = new HashMap<>();
                for (final PropertyGroupConfiguration existingGroupConfig : existingGroupConfigs) {
                    existingGroupConfigMap.put(existingGroupConfig.groupName(), existingGroupConfig);
                }

                for (final PropertyGroupConfiguration override : propertyOverrides) {
                    final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>();

                    final PropertyGroupConfiguration existing = existingGroupConfigMap.get(override.groupName());
                    if (existing != null) {
                        mergedProperties.putAll(existing.propertyValues());
                    }
                    mergedProperties.putAll(override.propertyValues());

                    createdGroupConfigs.add(new PropertyGroupConfiguration(override.groupName(), mergedProperties));
                }

                created.setProperties(stepName, createdGroupConfigs);
            }

            return created;
        } finally {
            readLock.unlock();
        }
    }


    @Override
    public ConfigurationUpdateResult setProperties(final String stepName, final List<PropertyGroupConfiguration> propertyGroupConfigurations) {
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

    @Override
    public ConfigurationUpdateResult replaceProperties(final String stepName, final List<PropertyGroupConfiguration> propertyGroupConfigurations) {
        writeLock.lock();
        try {
            final List<PropertyGroupConfiguration> existingGroupConfigs = this.propertyGroupConfigurations.get(stepName);
            if (Objects.equals(existingGroupConfigs, propertyGroupConfigurations)) {
                return ConfigurationUpdateResult.NO_CHANGES;
            }

            this.propertyGroupConfigurations.put(stepName, copyPropertyGroupConfigurations(propertyGroupConfigurations));
            return ConfigurationUpdateResult.CHANGES_MADE;
        } finally {
            writeLock.unlock();
        }
    }

    private List<PropertyGroupConfiguration> copyPropertyGroupConfigurations(final List<PropertyGroupConfiguration> groupConfigs) {
        final List<PropertyGroupConfiguration> copiedConfigs = new ArrayList<>();

        for (final PropertyGroupConfiguration groupConfig : groupConfigs) {
            final Map<String, ConnectorValueReference> copiedProperties = new HashMap<>(groupConfig.propertyValues());
            copiedConfigs.add(new PropertyGroupConfiguration(groupConfig.groupName(), copiedProperties));
        }

        return copiedConfigs;
    }

    @Override
    public ConnectorConfiguration toConnectorConfiguration() {
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
            mergedMap.put(groupConfig.groupName(), groupConfig);
        }

        for (final PropertyGroupConfiguration groupConfig : newGroupConfigs) {
            final PropertyGroupConfiguration existingConfiguration = mergedMap.get(groupConfig.groupName());
            final PropertyGroupConfiguration mergedGroupConfig = merge(existingConfiguration, groupConfig);
            mergedMap.put(groupConfig.groupName(), mergedGroupConfig);
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

        final Map<String, ConnectorValueReference> mergedProperties = new HashMap<>(existingConfiguration.propertyValues());
        mergedProperties.putAll(newConfiguration.propertyValues());
        return new PropertyGroupConfiguration(existingConfiguration.groupName(), mergedProperties);
    }

    public MutableConnectorConfigurationContext clone() {
        readLock.lock();
        try {
            final StandardConnectorConfigurationContext cloned = new StandardConnectorConfigurationContext();
            for (final Map.Entry<String, List<PropertyGroupConfiguration>> entry : this.propertyGroupConfigurations.entrySet()) {
                final String stepName = entry.getKey();
                final List<PropertyGroupConfiguration> groupConfigurations = entry.getValue();

                final List<PropertyGroupConfiguration> clonedGroupConfigs = new ArrayList<>();
                for (final PropertyGroupConfiguration groupConfig : groupConfigurations) {
                    final Map<String, ConnectorValueReference> clonedProperties = new HashMap<>(groupConfig.propertyValues());
                    clonedGroupConfigs.add(new PropertyGroupConfiguration(groupConfig.groupName(), clonedProperties));
                }

                cloned.propertyGroupConfigurations.put(stepName, clonedGroupConfigs);
            }

            return cloned;
        } finally {
            readLock.unlock();
        }
    }
}
