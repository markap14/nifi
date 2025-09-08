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
package org.apache.nifi.nar;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.Connector;
import org.apache.nifi.components.connector.ConnectorInitializationContext;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;

import java.util.List;
import java.util.Map;

@Tags({"test", "connector"})
public class DummyConnector implements Connector {
    private ConnectorInitializationContext context;

    @Override
    public void initialize(final ConnectorInitializationContext context) {
        this.context = context;
    }

    @Override
    public void start() throws FlowUpdateException {
        // no-op
    }

    @Override
    public void stop() throws FlowUpdateException {
        // no-op
    }

    @Override
    public List<ValidationResult> validate() {
        return List.of();
    }

    @Override
    public List<String> getPropertyGroupNames() {
        return List.of();
    }

    @Override
    public ConnectorPropertyGroup getPropertyGroup(final String groupName) {
        return null;
    }

    @Override
    public void onConfigured() throws FlowUpdateException {
        // no-op
    }

    @Override
    public void onPropertyGroupConfigured(final String groupName) {
        // no-op
    }

    @Override
    public List<ValidationResult> validatePropertyGroup(final String groupName, final Map<String, String> propertyValues) {
        return List.of();
    }

    public ConnectorInitializationContext getContext() {
        return context;
    }
}


