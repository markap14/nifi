/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.ConfigVerificationResult;

import java.util.List;

public interface ConnectorConfigVerificationResult {

    List<ConfigVerificationResult> getAllResults();

    List<ConfigVerificationResult> getFailedResults();

    void assertNoFailures();
}
