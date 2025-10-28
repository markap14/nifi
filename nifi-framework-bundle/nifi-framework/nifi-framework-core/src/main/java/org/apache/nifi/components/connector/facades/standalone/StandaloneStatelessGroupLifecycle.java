/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.StatelessGroupLifecycle;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StatelessGroupNode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class StandaloneStatelessGroupLifecycle implements StatelessGroupLifecycle {
    private final StatelessGroupNode statelessGroupNode;
    private final ProcessScheduler processScheduler;

    public StandaloneStatelessGroupLifecycle(final ProcessGroup processGroup, final ProcessScheduler processScheduler) {
        this.statelessGroupNode = processGroup.getStatelessGroupNode().orElseThrow(() -> new IllegalStateException("Process Group is not configured to run using the Stateless Execution Engine"));
        this.processScheduler = processScheduler;
    }

    @Override
    public CompletableFuture<Void> start() {
        return processScheduler.startStatelessGroup(statelessGroupNode);
    }

    @Override
    public CompletableFuture<Void> stop() {
        return processScheduler.stopStatelessGroup(statelessGroupNode);
    }

    // TODO: Stateless Group does not currently support termination.
    @Override
    public CompletableFuture<Void> terminate() {
        return CompletableFuture.completedFuture(null);
    }
}
