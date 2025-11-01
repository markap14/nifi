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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;

public class StandardConnectorInitializationContext implements ConnectorInitializationContext {
    private final String identifier;
    private final String name;
    private final ComponentLog componentLog;
    private final ProcessGroup managedProcessGroup;
    private final SecretsManager secretsManager;
    private final Bundle configuredBundle;
    private final Bundle activeBundle;
    private final AssetManager assetManager;
    private final FlowContextFactory flowContextFactory;

    private volatile FlowContext activeFlowContext;
    private volatile FlowContext workingFlowContext;

    private StandardConnectorInitializationContext(final Builder builder) {
        this.identifier = builder.identifier;
        this.name = builder.name;
        this.componentLog = builder.componentLog;
        this.managedProcessGroup = builder.managedProcessGroup;
        this.flowContextFactory = builder.flowContextFactory;
        this.secretsManager = builder.secretsManager;
        this.configuredBundle = builder.configuredBundle;
        this.activeBundle = builder.activeBundle;
        this.assetManager = builder.assetManager;

        this.activeFlowContext = flowContextFactory.createActiveFlowContext(managedProcessGroup, componentLog);
        this.workingFlowContext = flowContextFactory.createWorkingFlowContext(componentLog);
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
    public FlowContext getActiveFlowContext() {
        return activeFlowContext;
    }

    @Override
    public FlowContext getWorkingFlowContext() {
        return workingFlowContext;
    }

    @Override
    public SecretsManager getSecretsManager() {
        return secretsManager;
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

        final ConnectorParameterLookup parameterLookup = new ConnectorParameterLookup(versionedExternalFlow.getParameterContexts().values(), assetManager);
        getActiveFlowContext().getParameterContext().updateParameters(parameterLookup.getParameterValues());

        activeFlowContext = flowContextFactory.createActiveFlowContext(managedProcessGroup, componentLog);
    }

    private void updateParameterContext(final VersionedProcessGroup group, final String parameterContextName) {
        group.setParameterContextName(parameterContextName);
        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                updateParameterContext(childGroup, parameterContextName);
            }
        }
    }

    @Override
    public Bundle getConfiguredBundle() {
        return configuredBundle;
    }

    @Override
    public Bundle getBundle() {
        return activeBundle;
    }


    public static class Builder implements ConnectorInitializationContextBuilder {
        private String identifier;
        private String name;
        private ComponentLog componentLog;
        private ProcessGroup managedProcessGroup;
        private SecretsManager secretsManager;
        private FlowContextFactory flowContextFactory;
        private Bundle configuredBundle;
        private Bundle activeBundle;
        private AssetManager assetManager;

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

        public Builder flowContextFactory(final FlowContextFactory flowContextFactory) {
            this.flowContextFactory = flowContextFactory;
            return this;
        }

        public Builder secretsManager(final SecretsManager secretsManager) {
            this.secretsManager = secretsManager;
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

        public Builder assetManager(final AssetManager assetManager) {
            this.assetManager = assetManager;
            return this;
        }

        public StandardConnectorInitializationContext build() {
            return new StandardConnectorInitializationContext(this);
        }
    }
}
