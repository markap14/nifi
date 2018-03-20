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

public interface WorkspaceSerializer {

    /**
     * Writes the given workspace to the specified location.
     *
     * @param workspace the workspace to serialize
     * @param location the location to write the serialized flow to
     * @throws IOException if unable to write the flow to the specified location
     */
    void serialize(Workspace workspace, String location) throws IOException;

}
