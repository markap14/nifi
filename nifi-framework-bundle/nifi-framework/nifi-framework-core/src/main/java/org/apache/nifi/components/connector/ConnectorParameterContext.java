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

package org.apache.nifi.components.connector;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.parameter.ParameterProviderLookup;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterProviderConfiguration;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterReferenceManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectorParameterContext implements ParameterContext {

    private final ConnectorNode connectorNode;
    private final ParameterReferenceManager parameterReferenceManager;

    private final Map<String, Parameter> parameterValues = new HashMap<>();
    private final AtomicLong revision = new AtomicLong(0L);

    public ConnectorParameterContext(final ConnectorNode connectorNode) {
        this.connectorNode = connectorNode;
        this.parameterReferenceManager = new StandardParameterReferenceManager(connectorNode::getManagedProcessGroup);
    }

    @Override
    public String getIdentifier() {
        return connectorNode.getIdentifier();
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public String getName() {
        return connectorNode.getName();
    }

    @Override
    public void setName(final String name) {
    }

    @Override
    public String getDescription() {
        return "Implicit Parameter Context for Connector " + connectorNode;
    }

    @Override
    public void setDescription(final String description) {
    }

    @Override
    public synchronized void setParameters(final Map<String, Parameter> updatedParameters) {
        parameterValues.clear();
        parameterValues.putAll(updatedParameters);
        revision.incrementAndGet();
    }

    @Override
    public synchronized Optional<Parameter> getParameter(final String parameterName) {
        return Optional.ofNullable(parameterValues.get(parameterName));
    }

    @Override
    public void verifyCanSetParameters(final Map<String, Parameter> parameters) {
    }

    @Override
    public Optional<Parameter> getParameter(final ParameterDescriptor parameterDescriptor) {
        return getParameter(parameterDescriptor.getName());
    }

    @Override
    public Map<ParameterDescriptor, Parameter> getParameters() {
        final Map<ParameterDescriptor, Parameter> paramMap = new HashMap<>();
        for (final Parameter parameter : this.parameterValues.values()) {
            paramMap.put(parameter.getDescriptor(), parameter);
        }
        return paramMap;
    }

    @Override
    public Map<ParameterDescriptor, Parameter> getEffectiveParameters() {
        return getParameters();
    }

    /**
     * Calculates what the parameters would be if the proposed changes were applied.
     */
    private Map<ParameterDescriptor, Parameter> getProposedParameters(final Map<String, Parameter> proposedParameterUpdates) {
        final Map<ParameterDescriptor, Parameter> proposedParameters = new HashMap<>(getParameters());
        
        for (final Map.Entry<String, Parameter> entry : proposedParameterUpdates.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();
            
            if (parameter == null) {
                // Remove parameter - find and remove by name
                proposedParameters.entrySet().removeIf(paramEntry -> 
                    paramEntry.getKey().getName().equals(parameterName));
            } else {
                // Add/update parameter - remove any existing with same name first
                proposedParameters.entrySet().removeIf(paramEntry -> 
                    paramEntry.getKey().getName().equals(parameterName));
                proposedParameters.put(parameter.getDescriptor(), parameter);
            }
        }
        
        return proposedParameters;
    }

    @Override
    public Map<String, Parameter> getEffectiveParameterUpdates(final Map<String, Parameter> parameters, final List<ParameterContext> inheritedParameterContexts) {
        if (parameters == null) {
            throw new IllegalArgumentException("Parameter Updates must be specified");
        }

        // Since this implementation doesn't support inheritance, we ignore inheritedParameterContexts
        // and just compare current parameters with proposed parameters
        final Map<ParameterDescriptor, Parameter> currentParameters = getParameters();
        final Map<ParameterDescriptor, Parameter> proposedParameters = getProposedParameters(parameters);

        return getEffectiveParameterUpdates(currentParameters, proposedParameters);
    }

    /**
     * Compares current effective parameters with proposed effective parameters to determine what actually changes.
     * Returns a map of parameter name to Parameter, where null values indicate parameter deletion.
     */
    private Map<String, Parameter> getEffectiveParameterUpdates(final Map<ParameterDescriptor, Parameter> currentEffectiveParameters,
                                                               final Map<ParameterDescriptor, Parameter> effectiveProposedParameters) {
        final Map<String, Parameter> effectiveParameterUpdates = new HashMap<>();
        
        // Check for new and updated parameters
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : effectiveProposedParameters.entrySet()) {
            final ParameterDescriptor proposedParameterDescriptor = entry.getKey();
            final Parameter proposedParameter = entry.getValue();
            
            if (currentEffectiveParameters.containsKey(proposedParameterDescriptor)) {
                final Parameter currentParameter = currentEffectiveParameters.get(proposedParameterDescriptor);
                // Check if parameter actually changed (value, sensitivity, or description)
                if (!currentParameter.equals(proposedParameter) || 
                    currentParameter.getDescriptor().isSensitive() != proposedParameter.getDescriptor().isSensitive() ||
                    !Objects.equals(currentParameter.getDescriptor().getDescription(), proposedParameter.getDescriptor().getDescription())) {
                    // The parameter has been updated in some way
                    effectiveParameterUpdates.put(proposedParameterDescriptor.getName(), proposedParameter);
                }
            } else {
                // It's a new parameter
                effectiveParameterUpdates.put(proposedParameterDescriptor.getName(), proposedParameter);
            }
        }
        
        // Check for removed parameters
        for (final Map.Entry<ParameterDescriptor, Parameter> entry : currentEffectiveParameters.entrySet()) {
            final ParameterDescriptor currentParameterDescriptor = entry.getKey();
            // If a current parameter is not in the proposed parameters, it was effectively removed
            if (!effectiveProposedParameters.containsKey(currentParameterDescriptor)) {
                effectiveParameterUpdates.put(currentParameterDescriptor.getName(), null);
            }
        }
        
        return effectiveParameterUpdates;
    }

    @Override
    public ParameterReferenceManager getParameterReferenceManager() {
        return parameterReferenceManager;
    }

    @Override
    public ParameterProviderLookup getParameterProviderLookup() {
        return null;
    }

    @Override
    public ParameterProvider getParameterProvider() {
        return null;
    }

    @Override
    public ParameterProviderConfiguration getParameterProviderConfiguration() {
        return null;
    }

    @Override
    public void configureParameterProvider(final ParameterProviderConfiguration parameterProviderConfiguration) {
    }

    @Override
    public void verifyCanUpdateParameterContext(final Map<String, Parameter> parameterUpdates, final List<ParameterContext> inheritedParameterContexts) {

    }

    @Override
    public void setInheritedParameterContexts(final List<ParameterContext> inheritedParameterContexts) {
        if (inheritedParameterContexts != null && !inheritedParameterContexts.isEmpty()) {
            throw new UnsupportedOperationException("ConnectorParameterContext does not support inherited Parameter Contexts");
        }
    }

    @Override
    public List<ParameterContext> getInheritedParameterContexts() {
        return List.of();
    }

    @Override
    public List<String> getInheritedParameterContextNames() {
        return List.of();
    }

    @Override
    public boolean hasReferencingComponents(final Parameter parameter) {
        final ParameterReferenceManager referenceManager = getParameterReferenceManager();
        final String parameterName = parameter.getDescriptor().getName();
        final Set<ProcessorNode> processors = referenceManager.getProcessorsReferencing(this, parameterName);
        if (!processors.isEmpty()) {
            return true;
        }

        final Set<ControllerServiceNode> services = referenceManager.getControllerServicesReferencing(this, parameterName);
        if (!services.isEmpty()) {
            return true;
        }

        return false;
    }

    @Override
    public boolean inheritsFrom(final String parameterContextId) {
        return false;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return connectorNode;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ParameterContext, getIdentifier(), getName());
    }

    @Override
    public boolean isEmpty() {
        return parameterValues.isEmpty();
    }

    @Override
    public long getVersion() {
        return revision.get();
    }
}
