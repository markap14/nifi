/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MockStateManager implements StateManager {
    private final AtomicLong versionCounter = new AtomicLong(0L);
    private final boolean dropStateKeySupported;

    private volatile StateMap localStateMap = new MockStateMap(null, -1L);
    private volatile StateMap clusterStateMap = new MockStateMap(null, -1L);

    public MockStateManager(final boolean dropStateKeySupported) {
        this.dropStateKeySupported = dropStateKeySupported;
    }

    @Override
    public synchronized void setState(final Map<String, String> state, final Scope scope) throws IOException {
        final StateMap newStateMap = new MockStateMap(state, versionCounter.incrementAndGet());

        if (scope == Scope.CLUSTER) {
            clusterStateMap = newStateMap;
        } else {
            localStateMap = newStateMap;
        }
    }

    @Override
    public synchronized StateMap getState(final Scope scope) throws IOException {
        if (scope == Scope.CLUSTER) {
            return clusterStateMap;
        } else {
            return localStateMap;
        }
    }

    @Override
    public synchronized boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        final StateMap currentStateMap = scope == Scope.CLUSTER ? clusterStateMap : localStateMap;

        if (currentStateMap == oldValue) {
            final StateMap newStateMap = new MockStateMap(newValue, versionCounter.incrementAndGet());

            if (scope == Scope.CLUSTER) {
                clusterStateMap = newStateMap;
            } else {
                localStateMap = newStateMap;
            }

            return true;
        }

        return false;
    }

    @Override
    public synchronized void clear(final Scope scope) throws IOException {
        setState(Collections.emptyMap(), scope);
    }

    @Override
    public boolean isStateKeyDropSupported() {
        return dropStateKeySupported;
    }
}

