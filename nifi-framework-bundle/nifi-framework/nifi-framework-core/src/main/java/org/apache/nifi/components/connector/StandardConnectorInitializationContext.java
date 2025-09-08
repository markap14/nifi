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
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;

public class StandardConnectorInitializationContext implements ConnectorInitializationContext {
    private final String identifier;
    private final String name;
    private final ComponentLog componentLog;
    private final ProcessGroupFacade processGroupFacade;
    private final SecretsManager secretsManager;
    private final ConnectorConfigurationContext connectorConfigurationContext;
    private final ParameterContextFacade parameterContextFacade;
    private final Bundle configuredBundle;
    private final Bundle activeBundle;

    private StandardConnectorInitializationContext(final Builder builder) {
        this.identifier = builder.identifier;
        this.name = builder.name;
        this.componentLog = builder.componentLog;
        this.processGroupFacade = builder.processGroupFacade;
        this.secretsManager = builder.secretsManager;
        this.connectorConfigurationContext = builder.connectorConfigurationContext;
        this.parameterContextFacade = builder.parameterContextFacade;
        this.configuredBundle = builder.configuredBundle;
        this.activeBundle = builder.activeBundle;
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
    public void updateFlow(final VersionedProcessGroup versionedProcessGroup, final FlowDrain flowDrain) {
        // TODO: Implement flow update logic
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
        private ProcessGroupFacade processGroupFacade;
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

        public Builder processGroupFacade(final ProcessGroupFacade processGroupFacade) {
            this.processGroupFacade = processGroupFacade;
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
