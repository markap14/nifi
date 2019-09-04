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
package org.apache.nifi.python;

import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Python4JExtensionManager implements ExtensionDiscoveringManager {
    private static final Logger logger = LoggerFactory.getLogger(Python4JExtensionManager.class);
    private static final BundleCoordinate PYTHON_BUNDLE_COORDINATES = new BundleCoordinate("org.apache.nifi", "python", "1.0.0-beta");
    private static final BundleDetails PYTHON_BUNDLE_DETAILS = new BundleDetails.Builder()
        .coordinate(PYTHON_BUNDLE_COORDINATES)
        .workingDir(new File("python/extensions"))
        .build();
    private static final Bundle PYTHON_BUNDLE = new Bundle(PYTHON_BUNDLE_DETAILS, Python4JExtensionManager.class.getClassLoader());


    private final ExtensionDiscoveringManager delegate;
    private final Set<String> discoveredModules = new HashSet<>();
    private final ConcurrentMap<String, InstanceClassLoader> instanceClassLoaders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConfigurableComponent> tempComponents = new ConcurrentHashMap<>();
    private final Set<ExtensionDefinition> pythonProcessorDefinitions = new HashSet<>();
    private final PythonBridge pythonBridge;


    public Python4JExtensionManager(final ExtensionDiscoveringManager toWrap) {
        this.delegate = toWrap;
        pythonBridge = new PythonBridge(9999);
    }

    @Override
    public void discoverExtensions(final Bundle systemBundle, final Set<Bundle> narBundles) {
        delegate.discoverExtensions(systemBundle, narBundles);

        discoverPythonModules();
    }

    @Override
    public void discoverExtensions(final Set<Bundle> narBundles) {
        delegate.discoverExtensions(narBundles);

        discoverPythonModules();
    }

    @Override
    public Set<Bundle> getAllBundles() {
        final Set<Bundle> bundles = delegate.getAllBundles();
        bundles.add(PYTHON_BUNDLE);
        return bundles;
    }

    @Override
    public InstanceClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle, final Set<URL> additionalUrls) {
        if (PYTHON_BUNDLE.equals(bundle)) {
            final InstanceClassLoader classLoader = new InstanceClassLoader(UUID.randomUUID().toString(), classType, null, null, PYTHON_BUNDLE.getClassLoader());
            instanceClassLoaders.put(classLoader.getIdentifier(), classLoader);
            return classLoader;
        } else {
            return delegate.createInstanceClassLoader(classType, instanceIdentifier, bundle, additionalUrls);
        }
    }

    @Override
    public InstanceClassLoader getInstanceClassLoader(final String instanceIdentifier) {
        final InstanceClassLoader classLoader = instanceClassLoaders.get(instanceIdentifier);
        if (classLoader == null) {
            return delegate.getInstanceClassLoader(instanceIdentifier);
        }

        return classLoader;
    }

    @Override
    public InstanceClassLoader removeInstanceClassLoader(final String instanceIdentifier) {
        final InstanceClassLoader classLoader = instanceClassLoaders.remove(instanceIdentifier);
        if (classLoader == null) {
            return delegate.removeInstanceClassLoader(instanceIdentifier);
        }

        return classLoader;
    }

    @Override
    public void closeURLClassLoader(final String instanceIdentifier, final ClassLoader classLoader) {
        delegate.closeURLClassLoader(instanceIdentifier, classLoader);
    }

    @Override
    public List<Bundle> getBundles(final String classType) {
        final List<Bundle> bundles = delegate.getBundles(classType);

        if (discoveredModules.contains(classType)) {
            bundles.add(PYTHON_BUNDLE);
        }

        return bundles;
    }

    @Override
    public Bundle getBundle(final BundleCoordinate bundleCoordinate) {
        return PYTHON_BUNDLE_COORDINATES.equals(bundleCoordinate) ? PYTHON_BUNDLE : delegate.getBundle(bundleCoordinate);
    }

    @Override
    public Set<Class> getTypes(final BundleCoordinate bundleCoordinate) {
        // TODO: This means that we won't get any docs
        return delegate.getTypes(bundleCoordinate);
    }

    @Override
    public Bundle getBundle(final ClassLoader classLoader) {
        return PYTHON_BUNDLE.getClassLoader().equals(classLoader) ? PYTHON_BUNDLE : delegate.getBundle(classLoader);
    }

    @Override
    public Set<Class> getExtensions(final Class<?> definition) {
        return delegate.getExtensions(definition);
    }

    @Override
    public Set<ExtensionDefinition> getProcessorDefinitions() {
        final Set<ExtensionDefinition> delegateDefintiions = delegate.getProcessorDefinitions();
        final Set<ExtensionDefinition> allDefinitions = new HashSet<>(delegateDefintiions);
        allDefinitions.addAll(pythonProcessorDefinitions);
        return allDefinitions;
    }

    @Override
    public ConfigurableComponent getTempComponent(final String classType, final BundleCoordinate bundleCoordinate) {
        if (PYTHON_BUNDLE_COORDINATES.equals(bundleCoordinate)) {
            return tempComponents.get(classType);
        }

        return delegate.getTempComponent(classType, bundleCoordinate);
    }

    @Override
    public void logClassLoaderMapping() {
        if (discoveredModules.isEmpty()) {
            logger.info("No Python Processors were discovered.");
            return;
        }

        logger.info("The following Python Processors were discovered:");
        discoveredModules.forEach(logger::info);
    }

    private void discoverPythonModules() {
        final String[] moduleNames = pythonBridge.getPythonController().getModules();

        for (final String moduleName : moduleNames) {
            discoveredModules.add(moduleName);

            try {
                tempComponents.put(moduleName, createTempComponent(moduleName));
            } catch (final Exception e) {
                logger.error("Failed to instantiate instance of Python Module <{}>", moduleName, e);
            }

            pythonProcessorDefinitions.add(createExtensionDefinition(moduleName));
        }
    }

    private PythonProcessor createTempComponent(final String moduleName) {
        final String identifier = UUID.randomUUID().toString();
        final PythonController controller = pythonBridge.getPythonController();
        final FlowFileFunction flowFileFunction = controller.createProcessor(identifier, moduleName);
        return new PythonProcessor(flowFileFunction);
    }

    private ExtensionDefinition createExtensionDefinition(final String moduleName) {
        return new ExtensionDefinition() {
            @Override
            public ExtensionType getExtensionType() {
                return ExtensionType.PROCESSOR;
            }

            @Override
            public Class<?> getExtensionClass() {
                return FlowFileFunction.class;
            }

            @Override
            public Bundle getBundle() {
                return PYTHON_BUNDLE;
            }

            @Override
            public String getExtensionName() {
                return moduleName;
            }

            @Override
            public String getCapabilityDescription() {
                return null;
            }

            @Override
            public boolean isRestricted() {
                return false;
            }

            @Override
            public String getUsageRestriction() {
                return null;
            }

            @Override
            public Set<Restriction> getExplicitRestrictions() {
                return Collections.emptySet();
            }

            @Override
            public String getDeprecationReason() {
                return null;
            }

            @Override
            public Collection<String> getTags() {
                return Collections.singleton("python");
            }
        };
    }
}
