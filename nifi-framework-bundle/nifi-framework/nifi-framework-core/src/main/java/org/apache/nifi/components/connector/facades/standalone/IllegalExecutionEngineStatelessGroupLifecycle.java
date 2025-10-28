/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.groups.ProcessGroup;

import java.util.concurrent.CompletableFuture;

public class IllegalExecutionEngineStatelessGroupLifecycle implements StatelessGroupLifecycle {
    private final ProcessGroup processGroup;

    public IllegalExecutionEngineStatelessGroupLifecycle(final ProcessGroup processGroup) {
        this.processGroup = processGroup;
    }

    @Override
    public CompletableFuture<Void> start() {
        throw new IllegalStateException("Cannot start " + processGroup + " as a Stateless Group because the Process Group is not configured to run using the Stateless Execution Engine");
    }

    @Override
    public CompletableFuture<Void> stop() {
        throw new IllegalStateException("Cannot stop " + processGroup + " as a Stateless Group because the Process Group is not configured to run using the Stateless Execution Engine");
    }

    @Override
    public CompletableFuture<Void> terminate() {
        throw new IllegalStateException("Cannot terminate " + processGroup + " as a Stateless Group because the Process Group is not configured to run using the Stateless Execution Engine");
    }
}
