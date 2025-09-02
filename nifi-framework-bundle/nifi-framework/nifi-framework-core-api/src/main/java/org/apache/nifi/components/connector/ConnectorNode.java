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

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.VersionedComponent;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public interface ConnectorNode extends ComponentAuthorizable, VersionedComponent {

    String getName();

    void setName(String name);

    String getDescription();

    void setDescription(String description);

    ConnectorConfiguration getConfiguration();

    void setConfiguration(ConnectorConfiguration configuration);

    ScheduledState getCurrentState();

    ScheduledState getDesiredState();

    void enable();

    void disable();

    Future<Void> start(ScheduledExecutorService scheduler);

    Future<Void> stop(ScheduledExecutorService scheduler);

    /**
     * Verifies that the Connector is in a state that allows it to be deleted. If the Connector is not in a state
     * that allows it to be deleted, this method will throw an IllegalStateException.
     *
     * @throws IllegalStateException if the Connector is not in a state that allows it to be deleted
     */
    void verifyCanDelete();

    void verifyCanStart();

    Connector getConnector();

    /**
     * @return the fully qualified class name of the underlying Connector implementation
     */
    String getComponentType();

    void setParentProcessGroup(ProcessGroup processGroup);

    ProcessGroup getParentProcessGroup();

    BundleCoordinate getBundleCoordinate();

    /**
     * <p>
     * Pause triggering asynchronous validation to occur when the connector is updated. Often times, it is necessary
     * to update several aspects of a connector, such as the properties and annotation data, at once. When this occurs,
     * we don't want to trigger validation for each update, so we can follow the pattern:
     * </p>
     *
     * <pre>
     * <code>
     * connectorNode.pauseValidationTrigger();
     * try {
     *   connectorNode.setProperties(properties);
     *   connectorNode.setAnnotationData(annotationData);
     * } finally {
     *   connectorNode.resumeValidationTrigger();
     * }
     * </code>
     * </pre>
     *
     * <p>
     * When calling this method, it is imperative that {@link #resumeValidationTrigger()} is always called within a {@code finally} block to
     * ensure that validation occurs.
     * </p>
     */
    void pauseValidationTrigger();

    /**
     * Resume triggering asynchronous validation to occur when the connector is updated. This method is to be used in conjunction
     * with {@link #pauseValidationTrigger()} as illustrated in its documentation. When this method is called, if the connector's Validation Status
     * is {@link ValidationStatus#VALIDATING}, connector validation will immediately be triggered asynchronously.
     */
    void resumeValidationTrigger();

    ComponentLog getComponentLog();
}
