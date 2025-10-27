/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.connectors.kafkas3;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KafkaToS3IT {

    private static ConnectorTestRunner runner;

    @BeforeAll
    public static void setupTestRunner() {
        runner = new StandardConnectorTestRunner.Builder()
            .connectorClassName("org.apache.nifi.connectors.kafkas3.KafkaToS3")
            .narLibraryDirectory(new File("target/libDir"))
            .build();
        assertNotNull(runner);
    }

    @AfterAll
    public static void cleanup() throws IOException {
        if (runner != null) {
            runner.close();
        }
    }

    @Test
    public void testValidate() {
        final List<ValidationResult> validationResults = runner.validate();
        assertEquals(List.of(), validationResults);
    }
}
