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

package org.apache.nifi.mock.connectors;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.List;
import java.util.Map;

public class GenerateAndLog extends AbstractConnector {

    @Override
    protected void init() throws FlowUpdateException {
        final VersionedExternalFlow versionedExternalFlow = VersionedFlowUtils.loadFlowFromResource("flows/Generate_and_Update.json");
        getInitializationContext().updateFlow(versionedExternalFlow);
    }

    @Override
    protected void onStepConfigured(final String stepName) {
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> properties) {
        return List.of();
    }
}
