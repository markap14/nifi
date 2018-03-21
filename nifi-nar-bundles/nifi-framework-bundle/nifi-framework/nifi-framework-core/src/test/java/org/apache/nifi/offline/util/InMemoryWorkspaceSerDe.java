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

package org.apache.nifi.offline.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.offline.Workspace;
import org.apache.nifi.offline.WorkspaceDeserializer;
import org.apache.nifi.offline.WorkspaceSerializer;

public class InMemoryWorkspaceSerDe implements WorkspaceSerializer, WorkspaceDeserializer {
    private Map<String, Workspace> workspaces = new HashMap<>();

    @Override
    public void serialize(Workspace workspace, String location) throws IOException {
        workspaces.put(location, workspace);
    }

    @Override
    public Workspace getWorkspace(String location) throws IOException {
        return workspaces.get(location);
    }

    public Set<String> getStorageLocations() {
        return workspaces.keySet();
    }
}
