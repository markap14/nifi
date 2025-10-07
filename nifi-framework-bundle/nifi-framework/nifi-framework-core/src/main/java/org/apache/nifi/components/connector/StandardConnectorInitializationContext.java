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

import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardConnectorInitializationContext implements ConnectorInitializationContext {
    private final String identifier;
    private final String name;
    private final ComponentLog componentLog;
    private final ProcessGroup managedProcessGroup;
    private final ProcessGroupFacadeFactory processGroupFacadeFactory;
    private final SecretsManager secretsManager;
    private final ConnectorConfigurationContext connectorConfigurationContext;
    private final ParameterContextFacade parameterContextFacade;
    private final Bundle configuredBundle;
    private final Bundle activeBundle;

    private volatile ProcessGroupFacade processGroupFacade;


    private StandardConnectorInitializationContext(final Builder builder) {
        this.identifier = builder.identifier;
        this.name = builder.name;
        this.componentLog = builder.componentLog;
        this.managedProcessGroup = builder.managedProcessGroup;
        this.processGroupFacadeFactory = builder.processGroupFacadeFactory;
        this.secretsManager = builder.secretsManager;
        this.connectorConfigurationContext = builder.connectorConfigurationContext;
        this.parameterContextFacade = builder.parameterContextFacade;
        this.configuredBundle = builder.configuredBundle;
        this.activeBundle = builder.activeBundle;

        this.processGroupFacade = processGroupFacadeFactory.create(managedProcessGroup);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ComponentLog getLogger() {
        return componentLog;
    }

    @Override
    public ProcessGroupFacade getRootGroup() {
        return processGroupFacade;
    }

    @Override
    public SecretsManager getSecretsManager() {
        return secretsManager;
    }

    @Override
    public ConnectorConfigurationContext getConfigurationContext() {
        return connectorConfigurationContext;
    }

    @Override
    public ParameterContextFacade getParameterContext() {
        return parameterContextFacade;
    }

    @Override
    public void updateFlow(final VersionedProcessGroup versionedProcessGroup) throws FlowUpdateException {
        final VersionedExternalFlow versionedExternalFlow = createVersionedExternalFlow(versionedProcessGroup);

        try {
            managedProcessGroup.verifyCanUpdate(versionedExternalFlow, true, false);
        } catch (final IllegalStateException e) {
            throw new FlowUpdateException("Flow is not in a state that allows the requested updated", e);
        }

        managedProcessGroup.updateFlow(versionedExternalFlow, managedProcessGroup.getIdentifier(), false, true, true);

        processGroupFacade = processGroupFacadeFactory.create(managedProcessGroup);
    }

    @Override
    public void updateFlow(final VersionedExternalFlow versionedExternalFlow) throws FlowUpdateException {
        final String parameterContextName = managedProcessGroup.getParameterContext().getName();
        updateParameterContext(versionedExternalFlow.getFlowContents(), parameterContextName);

        try {
            managedProcessGroup.verifyCanUpdate(versionedExternalFlow, true, false);
        } catch (final IllegalStateException e) {
            throw new FlowUpdateException("Flow is not in a state that allows the requested updated", e);
        }

        managedProcessGroup.updateFlow(versionedExternalFlow, managedProcessGroup.getIdentifier(), false, true, true);

        final List<ParameterValue> parameterValues = createParameterValues(versionedExternalFlow.getParameterContexts().values());
        getParameterContext().updateParameters(parameterValues);

        processGroupFacade = processGroupFacadeFactory.create(managedProcessGroup);
    }

    private void updateParameterContext(final VersionedProcessGroup group, final String parameterContextName) {
        group.setParameterContextName(parameterContextName);
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                updateParameterContext(childGroup, parameterContextName);
            }
        }
    }

    /**
     * Converts a {@code List<VersionedParameterContext>} found in a VersionedExternalFlow to a
     * {@code List<ParameterValue>} that can be used to update a ParameterContext from a Connector,
     * respecting parameter context inheritance and precedence.
     *
     * @param parameterContexts the list of parameter contexts from a VersionedExternalFlow
     * @return the list of ParameterValues
     */
    static List<ParameterValue> createParameterValues(final Collection<VersionedParameterContext> parameterContexts) {
        final List<ParameterValue> parameterValues = new ArrayList<>();

        if (parameterContexts == null || parameterContexts.isEmpty()) {
            return parameterValues;
        }

        // Create a map for easy lookup of parameter contexts by name
        final Map<String, VersionedParameterContext> contextMap = new HashMap<>();
        for (final VersionedParameterContext context : parameterContexts) {
            contextMap.put(context.getName(), context);
        }

        // Process each parameter context, including inherited contexts
        final Set<String> processedContexts = new HashSet<>();
        for (final VersionedParameterContext context : parameterContexts) {
            collectParameterValues(context, contextMap, processedContexts, parameterValues);
        }

        return parameterValues;
    }

    private static void collectParameterValues(final VersionedParameterContext context,
        final Map<String, VersionedParameterContext> contextMap,
        final Set<String> processedContexts,
        final List<ParameterValue> parameterValues) {
        if (context == null || processedContexts.contains(context.getName())) {
            return;
        }

        processedContexts.add(context.getName());

        // Create a map to track existing parameters for efficient lookup
        final Map<String, ParameterValue> existingParametersByName = new HashMap<>();
        for (final ParameterValue existing : parameterValues) {
            existingParametersByName.put(existing.getName(), existing);
        }

        // First, process inherited parameter contexts in reverse order (lowest precedence first)
        // This ensures that the first inherited context (highest precedence) will override later ones
        if (context.getInheritedParameterContexts() != null && !context.getInheritedParameterContexts().isEmpty()) {
            final List<String> inheritedContextNames = context.getInheritedParameterContexts();
            // Process in reverse order so that the first (highest precedence) inherited context processes last
            for (int i = inheritedContextNames.size() - 1; i >= 0; i--) {
                final String inheritedContextName = inheritedContextNames.get(i);
                final VersionedParameterContext inheritedContext = contextMap.get(inheritedContextName);
                if (inheritedContext != null) {
                    collectParameterValues(inheritedContext, contextMap, processedContexts, parameterValues);
                }
            }
        }

        // Then, process this context's own parameters (they have the highest precedence and override all inherited ones)
        if (context.getParameters() != null) {
            // Rebuild the existing parameters map since inherited contexts may have added parameters
            existingParametersByName.clear();
            for (final ParameterValue existing : parameterValues) {
                existingParametersByName.put(existing.getName(), existing);
            }

            for (final VersionedParameter versionedParameter : context.getParameters()) {
                final String parameterName = versionedParameter.getName();

                // Remove existing parameter if present, then add the new one (current context overrides)
                if (existingParametersByName.containsKey(parameterName)) {
                    parameterValues.removeIf(param -> param.getName().equals(parameterName));
                }

                final ParameterValue paramValue = new ParameterValue.Builder()
                    .name(parameterName)
                    .value(versionedParameter.getValue())
                    .sensitive(versionedParameter.isSensitive())
                    .build();

                parameterValues.add(paramValue);
            }
        }
    }

    private VersionedExternalFlow createVersionedExternalFlow(final VersionedProcessGroup versionedProcessGroup) {
        final VersionedExternalFlow versionedExternalFlow = new VersionedExternalFlow();
        versionedExternalFlow.setFlowContents(versionedProcessGroup);
        versionedExternalFlow.setExternalControllerServices(Collections.emptyMap());
        versionedExternalFlow.setParameterProviders(Collections.emptyMap());

        final VersionedExternalFlowMetadata metadata = new VersionedExternalFlowMetadata();
        versionedExternalFlow.setMetadata(metadata);
        metadata.setFlowName(versionedProcessGroup.getName());
        metadata.setTimestamp(System.currentTimeMillis());
        metadata.setVersion("Unversioned");

        final Set<VersionedParameter> versionedParameters = new HashSet<>();
        for (final String parameterName : getParameterContext().getDefinedParameterNames()) {
            final String parameterValue = parameterContextFacade.getValue(parameterName);
            final VersionedParameter parameter = new VersionedParameter();
            parameter.setName(parameterName);
            parameter.setValue(parameterValue);
            parameter.setSensitive(parameterContextFacade.isSensitive(parameterName));
            versionedParameters.add(parameter);
        }

        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setInheritedParameterContexts(List.of());
        versionedParameterContext.setDescription("Implicit Parameter Context for Connector");
        versionedParameterContext.setName("implicit-parameter-context");
        versionedParameterContext.setIdentifier("implicit-parameter-context");
        versionedParameterContext.setParameters(versionedParameters);
        final Map<String, VersionedParameterContext> parameterContextMap = Map.of("implicit-parameter-context", versionedParameterContext);
        versionedExternalFlow.setParameterContexts(parameterContextMap);

        return versionedExternalFlow;
    }

    @Override
    public Bundle getConfiguredBundle() {
        return configuredBundle;
    }

    @Override
    public Bundle getBundle() {
        return activeBundle;
    }

    public static class Builder {
        private String identifier;
        private String name;
        private ComponentLog componentLog;
        private ProcessGroup managedProcessGroup;
        private ProcessGroupFacadeFactory processGroupFacadeFactory;
        private SecretsManager secretsManager;
        private ConnectorConfigurationContext connectorConfigurationContext;
        private ParameterContextFacade parameterContextFacade;
        private Bundle configuredBundle;
        private Bundle activeBundle;

        public Builder identifier(final String identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder componentLog(final ComponentLog componentLog) {
            this.componentLog = componentLog;
            return this;
        }

        public Builder managedProcessGroup(final ProcessGroup managedProcessGroup) {
            this.managedProcessGroup = managedProcessGroup;
            return this;
        }

        public Builder processGroupFacadeFactory(final ProcessGroupFacadeFactory processGroupFacadeFactory) {
            this.processGroupFacadeFactory = processGroupFacadeFactory;
            return this;
        }

        public Builder secretsManager(final SecretsManager secretsManager) {
            this.secretsManager = secretsManager;
            return this;
        }

        public Builder configurationContext(final ConnectorConfigurationContext connectorConfigurationContext) {
            this.connectorConfigurationContext = connectorConfigurationContext;
            return this;
        }

        public Builder parameterContextFacade(final ParameterContextFacade parameterContextFacade) {
            this.parameterContextFacade = parameterContextFacade;
            return this;
        }

        public Builder configuredBundle(final Bundle configuredBundle) {
            this.configuredBundle = configuredBundle;
            return this;
        }

        public Builder activeBundle(final Bundle activeBundle) {
            this.activeBundle = activeBundle;
            return this;
        }

        public StandardConnectorInitializationContext build() {
            return new StandardConnectorInitializationContext(this);
        }
    }
}
