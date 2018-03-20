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

package org.apache.nifi.offline;

import java.io.IOException;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

/**
 * <p>
 * An Offline Flow that is being developed in a Workspace.
 * </p>
 */
public interface OfflineFlow {

    /**
     * @return the current version of the Flow
     * @throws IOException if unable to read to understand flow contents
     */
    default VersionedProcessGroup getCurrentFlow() throws IOException {
        return getFlow(getCurrentRevision());
    }

    /**
     * @return the current revision of the flow
     */
    int getCurrentRevision();

    /**
     * @return the revision of the oldest version of the flow that is available
     */
    int getMinAvailableRevision();

    /**
     * @return the revision of the newest version of the flow
     */
    int getMaxRevision();

    /**
     * Returns the Process Group that represents the flow for the given revision
     *
     * @param revision the revision of the flow to retrieve
     * @return the VersionedProcessGroup that represents the flow for the given revision
     *
     * @throws IllegalArgumentException if the given revision is not valid for this OfflineFlow
     * @throws IOException if unable to read to understand flow contents
     */
    VersionedProcessGroup getFlow(int revision) throws IOException;
}
