/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;

import java.util.Collections;
import java.util.List;

public class MockServerConfigVerificationResult implements ConnectorConfigVerificationResult {
    private final List<ConfigVerificationResult> results;

    public MockServerConfigVerificationResult(final List<ConfigVerificationResult> results) {
        this.results = results;
    }

    @Override
    public List<ConfigVerificationResult> getAllResults() {
        return Collections.unmodifiableList(results);
    }

    @Override
    public List<ConfigVerificationResult> getFailedResults() {
        return results.stream()
            .filter(result -> result.getOutcome() == Outcome.FAILED)
            .toList();
    }

    @Override
    public void assertNoFailures() {
        final List<ConfigVerificationResult> failedResults = getFailedResults();
        if (!failedResults.isEmpty()) {
            throw new AssertionError("Configuration verification failed: " + failedResults);
        }
    }
}
