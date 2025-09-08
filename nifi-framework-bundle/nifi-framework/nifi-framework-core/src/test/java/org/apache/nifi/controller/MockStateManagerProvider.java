/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.controller;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.state.StandardStateMap;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;

public class MockStateManagerProvider implements StateManagerProvider {
    @Override
    public StateManager getStateManager(final String componentId) {
        final StateManager stateManager = Mockito.mock(StateManager.class);
        final StateMap emptyStateMap = new StandardStateMap(Collections.emptyMap(), Optional.empty());
        try {
            Mockito.when(stateManager.getState(any(Scope.class))).thenReturn(emptyStateMap);
        } catch (IOException e) {
            throw new AssertionError();
        }

        return stateManager;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void enableClusterProvider() {
    }

    @Override
    public void disableClusterProvider() {
    }

    @Override
    public void onComponentRemoved(final String componentId) {
    }
}
