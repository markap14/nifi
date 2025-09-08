/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
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
