/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface ConnectorTestRunner extends Closeable {

    void prepareForUpdate() throws FlowUpdateException;

    void finishUpdate() throws FlowUpdateException;

    void configure(String stepName, String propertyGroupName, Map<String, String> properties) throws FlowUpdateException;

    void startConnector();

    void stopConnector();

    void waitForDataIngested(Duration maxWaitTime);

    void waitForIdle(Duration minimumIdleTime, Duration maxWaitTime);

    List<ValidationResult> validate();

}
