/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ValidationResult;

import java.util.List;

public class GhostConnector implements  Connector {
    private final String identifier;
    private final String canonicalClassName;

    public GhostConnector(final String identifier, final String canonicalClassName) {
        this.identifier = identifier;
        this.canonicalClassName = canonicalClassName;
    }

    @Override
    public void initialize(final ConnectorInitializationContext connectorInitializationContext) {
    }

    @Override
    public void start() throws FlowUpdateException {
    }

    @Override
    public void stop() throws FlowUpdateException {
    }

    @Override
    public List<ValidationResult> validate() {
        return List.of(new ValidationResult.Builder()
            .subject("Missing Connector")
            .input("Any Property")
            .valid(false)
            .explanation("Connector is of type " + canonicalClassName + ", but this Connector implementation could not be created")
            .build());
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
    public void onConfigured() {
    }

    @Override
    public String toString() {
        return "GhostConnector[id=" + identifier + ", type=" + canonicalClassName + "]";
    }
}
