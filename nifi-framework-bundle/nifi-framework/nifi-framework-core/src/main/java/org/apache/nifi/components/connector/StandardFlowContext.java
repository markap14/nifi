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

import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;

public class StandardFlowContext implements FlowContext {
    private final ProcessGroupFacade rootGroup;
    private final ParameterContextFacade parameterContext;
    private final ConnectorConfigurationContext configurationContext;

    public StandardFlowContext(final ProcessGroupFacade rootGroup, final ParameterContextFacade parameterContext, final ConnectorConfigurationContext configurationContext) {
        this.rootGroup = rootGroup;
        this.parameterContext = parameterContext;
        this.configurationContext = configurationContext;
    }

    @Override
    public ProcessGroupFacade getRootGroup() {
        return rootGroup;
    }

    @Override
    public ParameterContextFacade getParameterContext() {
        return parameterContext;
    }

    @Override
    public ConnectorConfigurationContext getConfigurationContext() {
        return configurationContext;
    }
}
