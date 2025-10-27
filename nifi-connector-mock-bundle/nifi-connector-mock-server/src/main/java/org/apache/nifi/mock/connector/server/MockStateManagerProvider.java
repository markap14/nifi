/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockStateManagerProvider implements StateManagerProvider {
    private final ConcurrentMap<String, StateManager> stateManagers = new ConcurrentHashMap<>();
    private volatile boolean clusterProviderEnabled = false;

    @Override
    public StateManager getStateManager(final String componentId, final boolean dropStateKeySupported) {
        return stateManagers.computeIfAbsent(componentId, id -> new MockStateManager(dropStateKeySupported));
    }

    @Override
    public void onComponentRemoved(final String componentId) {
        stateManagers.remove(componentId);
    }

    @Override
    public void shutdown() {
        stateManagers.clear();
    }

    @Override
    public void enableClusterProvider() {
        clusterProviderEnabled = true;
    }

    @Override
    public void disableClusterProvider() {
        clusterProviderEnabled = false;
    }

    @Override
    public boolean isClusterProviderEnabled() {
        return clusterProviderEnabled;
    }
}
