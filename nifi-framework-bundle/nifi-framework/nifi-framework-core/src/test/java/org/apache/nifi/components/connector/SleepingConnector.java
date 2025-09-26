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

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class SleepingConnector implements Connector {
    private final Duration sleepDuration;

    public SleepingConnector() {
        this(Duration.ofMillis(100));
    }

    public SleepingConnector(Duration sleepDuration) {
        this.sleepDuration = sleepDuration;
    }

    @Override
    public void initialize(final ConnectorInitializationContext connectorInitializationContext) {
    }

    @Override
    public void start() throws FlowUpdateException {
        try {
            Thread.sleep(sleepDuration);
        } catch (final InterruptedException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public void stop() throws FlowUpdateException {
        try {
            Thread.sleep(sleepDuration);
        } catch (final InterruptedException e) {
            throw new FlowUpdateException(e);
        }
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
    public ConnectorPropertyGroup getPropertyGroup(final String s) {
        return null;
    }

    @Override
    public void onConfigured() throws FlowUpdateException {
        try {
            Thread.sleep(sleepDuration);
        } catch (final InterruptedException e) {
            throw new FlowUpdateException(e);
        }
    }

    @Override
    public void onPropertyGroupConfigured(final String groupName) {

    }

    @Override
    public List<ValidationResult> validatePropertyGroup(final String groupName, final Map<String, String> propertyValues) {
        return List.of();
    }
}
