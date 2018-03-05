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
import java.io.OutputStream;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

public interface OfflineFlowSerializer {

    /**
     * Writes the given flow to the given Output Stream. This method will not close the given OutputStream.
     *
     * @param flow the flow to serialize
     * @param destination the OutputStream to write the serialized flow to
     * @throws IOException if unable to write the flow to the output stream
     */
    void serialize(VersionedProcessGroup flow, OutputStream destination) throws IOException;

}
