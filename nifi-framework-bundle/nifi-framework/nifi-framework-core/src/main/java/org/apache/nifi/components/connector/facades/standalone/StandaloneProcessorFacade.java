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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.components.ProcessorLifecycle;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.parameter.ParameterContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StandaloneProcessorFacade implements ProcessorFacade {
    private final ProcessorNode processorNode;
    private final VersionedProcessor versionedProcessor;
    private final ParameterContext parameterContext;
    private final ProcessorLifecycle lifecycle;

    public StandaloneProcessorFacade(final ProcessorNode processorNode, final VersionedProcessor versionedProcessor, final ProcessScheduler scheduler,
            final ParameterContext parameterContext) {

        this.processorNode = processorNode;
        this.versionedProcessor = versionedProcessor;
        this.parameterContext = parameterContext;

        this.lifecycle = new StandaloneProcessorLifecycle(processorNode, scheduler);
    }

    @Override
    public VersionedProcessor getDefinition() {
        return versionedProcessor;
    }

    @Override
    public ProcessorLifecycle getLifecycle() {
        return lifecycle;
    }

    @Override
    public List<ValidationResult> validate(final Map<String, String> propertyValues) {
        final ValidationContext validationContext = processorNode.createValidationContext(propertyValues, processorNode.getAnnotationData(), parameterContext, true);
        final ValidationState validationState = processorNode.performValidation(validationContext);

        return switch(validationState.getStatus()) {
            case VALID -> Collections.emptyList();
            // If validating, return the current validation errors (if any)
            case INVALID, VALIDATING -> List.copyOf(validationState.getValidationErrors());
        };
    }

    @Override
    public Object invokeConnectorMethod(final String methodName, final Map<String, Object> arguments) throws InvocationFailedException {
        return processorNode.invokeConnectorMethod(methodName, arguments);
    }
}
