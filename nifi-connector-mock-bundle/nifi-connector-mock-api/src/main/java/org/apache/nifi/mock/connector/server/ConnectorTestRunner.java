/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyGroupConfiguration;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;

public interface ConnectorTestRunner extends Closeable {

    void prepareForUpdate() throws FlowUpdateException;

    void finishUpdate() throws FlowUpdateException;

    default void configure(String stepName, PropertyGroupConfiguration groupConfiguration) throws FlowUpdateException {
        configure(stepName, List.of(groupConfiguration));
    }

    void configure(String stepName, List<PropertyGroupConfiguration> groupConfigurations) throws FlowUpdateException;

    ConnectorConfigVerificationResult verifyConfiguration(String stepName, List<PropertyGroupConfiguration> groupConfigurations);

    void startConnector();

    void stopConnector();

    void waitForDataIngested(Duration maxWaitTime);

    void waitForIdle(Duration maxWaitTime);

    void waitForIdle(Duration minimumIdleTime, Duration maxWaitTime);

    List<ValidationResult> validate();

}
