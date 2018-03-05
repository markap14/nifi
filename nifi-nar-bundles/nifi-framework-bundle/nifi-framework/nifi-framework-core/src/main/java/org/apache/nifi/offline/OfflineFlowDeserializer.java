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
import java.io.InputStream;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

public interface OfflineFlowDeserializer {

    /**
     * Reads a flow from the given InputStream and returns it as a VersionedProcessGroup.
     * This method will not close the given InputStream but may read as much data as it chooses to,
     * even beyond the number of bytes necessary to deserialize the object. Therefore, it is up to the caller
     * to guard against this happening (e.g., by using a LimitingInputStream) if this is problematic.
     *
     * @param in the InputStream to read from
     * @return the VersionedProcessGroup that was read from the stream
     * @throws IOException if unable to read the necessary data from the stream or parse it
     */
    VersionedProcessGroup deserialize(InputStream in) throws IOException;

}
