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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.ConnectorUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DynamicAllowableValuesConnector extends AbstractConnector {
    static final String COLORS_GROUP_NAME = "Colors";

    static final ConnectorPropertyDescriptor FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name("File Path")
        .description("The path to the file")
        .required(true)
        .build();

    static final ConnectorPropertySubGroup FILE = new ConnectorPropertySubGroup.Builder()
        .name("")
        .addProperty(FILE_PATH)
        .build();

    static final ConnectorPropertyGroup FILE_GROUP = new ConnectorPropertyGroup.Builder()
        .name("File")
        .subGroups(List.of(FILE))
        .build();


    @Override
    public List<String> getPropertyGroupNames() {
        final List<String> groupNames = new ArrayList<>();
        groupNames.add(FILE_GROUP.getName());
        if (getProperty(FILE_GROUP, FILE_PATH) != null) {
            groupNames.add(COLORS_GROUP_NAME);
        }
        return groupNames;
    }

    @Override
    public ConnectorPropertyGroup getPropertyGroup(final String groupName) {
        if (groupName.equals(COLORS_GROUP_NAME)) {
            final Set<ProcessorFacade> processorsFacades = getInitializationContext().getRootGroup().getProcessors();
            if (processorsFacades.isEmpty()) {
                return null;
            }

            final ProcessorFacade processorFacade = processorsFacades.iterator().next();
            try {
                final List<String> fileValues = (List<String>) processorFacade.invokeConnectorMethod("getFileValues", Map.of());
                return createColorGroup(fileValues);
            } catch (final InvocationFailedException e) {
                throw new RuntimeException(e);
            }
        } else if (groupName.equals(FILE_GROUP.getName())) {
            return FILE_GROUP;
        }

        return null;
    }

    private ConnectorPropertyGroup createColorGroup(final List<String> values) {
        final ConnectorPropertyDescriptor FIRST_PRIMARY_COLOR = new ConnectorPropertyDescriptor.Builder()
            .name("First Primary Color")
            .description("The first primary color")
            .defaultValue(values.getFirst())
            .allowableValues(values.toArray(new String[0]))
            .required(true)
            .build();

        final ConnectorPropertySubGroup PRIMARY = new ConnectorPropertySubGroup.Builder()
            .name("Primary Colors")
            .addProperty(FIRST_PRIMARY_COLOR)
            .build();

        return new ConnectorPropertyGroup.Builder()
            .name("Colors")
            .subGroups(List.of(PRIMARY))
            .build();
    }

    @Override
    public void onConfigured() throws FlowUpdateException {
        final VersionedExternalFlow externalFlow = ConnectorUtils.loadFlowFromResource("flows/choose-color.json");
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();
        final VersionedProcessor processor = rootGroup.getProcessors().iterator().next();
        processor.setProperties(Map.of("File", getProperty(FILE_GROUP, FILE_PATH)));

        getInitializationContext().updateFlow(externalFlow, this::drainFlowFiles);
    }

    @Override
    public void onPropertyGroupConfigured(final String groupName) {
    }

    @Override
    public List<ValidationResult> validatePropertyGroup(final String groupName, final Map<String, String> propertyValues) {
        return List.of();
    }
}
