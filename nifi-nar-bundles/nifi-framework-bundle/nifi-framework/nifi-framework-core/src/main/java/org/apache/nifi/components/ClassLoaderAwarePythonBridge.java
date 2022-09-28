/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.components;

import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.python.BoundObjectCounts;
import org.apache.nifi.python.PythonBridge;
import org.apache.nifi.python.PythonBridgeInitializationContext;
import org.apache.nifi.python.PythonProcessorDetails;
import org.apache.nifi.python.processor.PythonProcessorBridge;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ClassLoaderAwarePythonBridge implements PythonBridge {
    private final PythonBridge delegate;
    private final ClassLoader classLoader;

    public ClassLoaderAwarePythonBridge(final PythonBridge delegate, final ClassLoader classLoader) {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public void initialize(final PythonBridgeInitializationContext context) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            delegate.initialize(context);
        }
    }

    @Override
    public void start() throws IOException {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            delegate.start();
        }
    }

    @Override
    public void shutdown() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            delegate.shutdown();
        }
    }

    @Override
    public void ping() throws IOException {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            delegate.ping();
        }
    }

    @Override
    public List<PythonProcessorDetails> getProcessorTypes() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            return delegate.getProcessorTypes();
        }
    }

    @Override
    public Map<String, Integer> getProcessCountsPerType() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            return delegate.getProcessCountsPerType();
        }
    }

    @Override
    public List<BoundObjectCounts> getBoundObjectCounts() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            return delegate.getBoundObjectCounts();
        }
    }

    @Override
    public List<String> getProcessorDependencies(final String processorType) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            return delegate.getProcessorDependencies(processorType);
        }
    }

    @Override
    public void discoverExtensions() {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            delegate.discoverExtensions();
        }
    }

    @Override
    public PythonProcessorBridge createProcessor(final String identifier, final String type, final String version, final boolean preferIsolatedProcess) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            return delegate.createProcessor(identifier, type, version, preferIsolatedProcess);
        }
    }

    @Override
    public void onProcessorRemoved(final String identifier, final String type, final String version) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(classLoader)) {
            delegate.onProcessorRemoved(identifier, type, version);
        }
    }
}
