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
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public interface ConnectorNode extends ComponentAuthorizable, VersionedComponent {

    String getName();

    String getDescription();

    ConnectorConfiguration getConfiguration() throws FlowUpdateException;

    ConnectorState getCurrentState();

    ConnectorState getDesiredState();

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

    ProcessGroup getManagedProcessGroup();

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

    ConnectorConfigurationContext getConfigurationContext();


    // -------------------
    // The following methods should always be called via the ConnectorRepository in order to maintain proper
    // lifecycle management of the Connector.

    /**
     * Sets the name of the Connector. This method should only be invoked via the ConnectorRepository.
     * @param name the Connector's name
     */
    void setName(String name);

    /**
     * Sets the description of the Connector. This method should only be invoked via the ConnectorRepository.
     * @param description the Connector's description
     */
    void setDescription(String description);

    /**
     * Enables the Connector. This method should only be invoked via the ConnectorRepository.
     */
    void enable();

    /**
     * Disables the Connector. This method should only be invoked via the ConnectorRepository.
     */
    void disable();

    /**
     * Starts the Connector. This method should only be invoked via the ConnectorRepository.
     * @param scheduler the ScheduledExecutorService to use for scheduling any tasks that the Connector needs to perform
     * @return a Future that will be completed when the Connector has started
     */
    Future<Void> start(ScheduledExecutorService scheduler);

    /**
     * Stops the Connector. This method should only be invoked via the ConnectorRepository.
     * @param scheduler the ScheduledExecutorService to use for scheduling any tasks that the Connector needs to perform
     * @return a Future that will be completed when the Connector has stopped
     */
    Future<Void> stop(ScheduledExecutorService scheduler);

    /**
     * Sets the configuration of the Connector. This method should only be invoked via the ConnectorRepository.
     * @param configuration the ConnectorConfiguration
     * @throws FlowUpdateException if unable to set the configuration
     */
    void setConfiguration(ConnectorConfiguration configuration) throws FlowUpdateException;

}
