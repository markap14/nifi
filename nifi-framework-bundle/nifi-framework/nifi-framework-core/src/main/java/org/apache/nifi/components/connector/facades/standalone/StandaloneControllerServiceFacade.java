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
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.ControllerServiceLifecycle;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.parameter.ParameterContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StandaloneControllerServiceFacade implements ControllerServiceFacade {
    private final ControllerServiceNode controllerServiceNode;
    private final VersionedControllerService versionedControllerService;
    private final ParameterContext parameterContext;
    private final ControllerServiceLifecycle lifecycle;

    public StandaloneControllerServiceFacade(final ControllerServiceNode controllerServiceNode, final VersionedControllerService versionedControllerService,
            final ParameterContext parameterContext, final ProcessScheduler processScheduler) {

        this.controllerServiceNode = controllerServiceNode;
        this.versionedControllerService = versionedControllerService;
        this.parameterContext = parameterContext;

        this.lifecycle = new StandaloneControllerServiceLifecycle(controllerServiceNode, processScheduler);
    }

    @Override
    public VersionedControllerService getDefinition() {
        return versionedControllerService;
    }

    @Override
    public ControllerServiceLifecycle getLifecycle() {
        return lifecycle;
    }

    // TODO: Refactor to avoid duplicate code with StandaloneProcessorFacade
    @Override
    public List<ValidationResult> validate(final Map<String, String> propertyValues) {
        final ValidationContext validationContext = controllerServiceNode.createValidationContext(propertyValues, controllerServiceNode.getAnnotationData(),
            parameterContext, true);
        final ValidationState validationState = controllerServiceNode.performValidation(validationContext);

        return switch(validationState.getStatus()) {
            case VALID -> Collections.emptyList();
            // If validating, return the current validation errors (if any)
            case INVALID, VALIDATING -> List.copyOf(validationState.getValidationErrors());
        };
    }

    @Override
    public Object invokeConnectorMethod(final String methodName, final Map<String, Object> arguments) throws InvocationFailedException {
        return controllerServiceNode.invokeConnectorMethod(methodName, arguments);
    }
}
