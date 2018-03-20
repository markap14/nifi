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
import java.util.Set;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.offline.exception.NoSuchWorkspaceException;
import org.apache.nifi.offline.exception.WorkspaceAlreadyExistsException;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.VersionedProcessGroup;

public interface WorkspaceRepository {

    /**
     * Initializes the repository, recovering any workspaces that exist
     *
     * @throws IOException if unable to read any of the workspaces
     */
    void initialize() throws IOException;

    /**
     * Returns a Set of all Workspaces that exist
     *
     * @return a Set of all Workspaces that exist
     */
    Set<Workspace> getWorkspaces();

    /**
     * Creates a new Workspace that can be used to update the flow for the given label.
     *
     * @param owner the owner of the workspace
     * @param label the label that the workspace applies to
     * @param c2Server the C2 Server that the label applies to
     * @param flowRegistry the Flow Registry to integrate with
     * @return the newly created Workspace
     *
     * @throws IOException if unable to communicate with the C2 Server or the Flow Registry or unable to persist the newly created Workspace.
     * @throws WorkspaceAlreadyExistsException if a workspace already exists for the given C2 Server and label.
     */
    Workspace createWorkspace(String owner, String label, C2Server c2Server, FlowRegistry flowRegistry) throws IOException, WorkspaceAlreadyExistsException;

    /**
     * Retrieves the Workspace with the given identifier, or returns null if no such workspace exists
     *
     * @param identifier the id of the workspace
     * @param user the owner of the workspace
     * @return the Workspace with the given ID or <code>null</code> if no such workspace exists
     *
     * @throws IOException if an IO failure occurs when attempting to access the workspace
     * @throws NoSuchWorkspaceException if no workspace exists with the given identifier
     * @throws AccessDeniedException if the given user is not the owner of the workspace
     */
    Workspace getWorkspace(String identifier, String user) throws IOException, NoSuchWorkspaceException;

    /**
     * Deletes the workspace with the given identifier
     *
     * @param identifier the id of the workspace
     * @param user the owner of the workspace
     *
     * @throws IOException if an IO failure occurs when attempting to delete the workspace
     * @throws AccessDeniedException if the given user is not the owner of the workspace
     * @throws NoSuchWorkspaceException if no workspace exists with the given identifier
     */
    void deleteWorkspace(String identifier, String user) throws IOException, NoSuchWorkspaceException;

    /**
     * Saves the given updated flow as the workspace's new flow, returning a new Workspace object that is updated
     * to reflect the newest version of the flow. The OfflineFlow that belongs to the Workspace that is returned will
     * have its max revision set to one more than the current revision. Any other versions of the flow whose revision
     * is greater than the current revision will immediately be removed.
     *
     * @param workspace the workspace to save
     * @param updatedFlow the new contents of the flow
     * @param user the user performing the action
     * @return a new Workspace object that is updated to reflect the newest version of the flow
     *
     * @throws IOException if unable to persist the data
     * @throws NoSuchWorkspaceException if the workspace no longer exists
     */
    Workspace saveWorkspace(Workspace workspace, VersionedProcessGroup updatedFlow, String user) throws IOException, NoSuchWorkspaceException;

    /**
     * Saves the flow so that it will now point to the given flow revision. This can be used to change which revision
     * of the OfflineFlow the workspace is using.
     *
     * @param workspace the workspace to save
     * @param flowRevision the revision of the flow that should be used
     * @param user the user performing the action
     * @return a new Workspace object that is updated to point to the given flow revision
     *
     * @throws IOException if unable to persist the data
     * @throws NoSuchWorkspaceException if no workspace exists with the given identifier
     * @throws IllegalArgumentException if the flowRevision is not valid for the given workspace
     */
    Workspace saveWorkspace(Workspace workspace, int flowRevision, String user) throws IOException, NoSuchWorkspaceException;
}
