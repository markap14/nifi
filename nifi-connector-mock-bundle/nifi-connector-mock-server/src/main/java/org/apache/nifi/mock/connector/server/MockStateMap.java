/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.state.StateMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MockStateMap implements StateMap {
    private final Map<String, String> stateValues;
    private final long version;

    public MockStateMap(final Map<String, String> stateValues, final long version) {
        this.stateValues = stateValues == null ? Collections.emptyMap() : new HashMap<>(stateValues);
        this.version = version;
    }

    @Override
    public Optional<String> getStateVersion() {
        return version == -1L ? Optional.empty() : Optional.of(Long.toString(version));
    }

    @Override
    public String get(final String key) {
        return stateValues.get(key);
    }

    @Override
    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(stateValues);
    }

    @Override
    public String toString() {
        return "MockStateMap[version=" + version + ", values=" + stateValues + "]";
    }
}

