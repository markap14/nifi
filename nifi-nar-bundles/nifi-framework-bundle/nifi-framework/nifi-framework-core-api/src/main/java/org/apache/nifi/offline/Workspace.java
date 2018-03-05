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

import org.apache.nifi.registry.flow.FlowRegistry;

/**
 * <p>
 * A Workspace is used to author an offline flow so that it can be pushed
 * to a Command & Control Server / Flow Registry. A Workspace maps to a single
 * Label on a single C2 Server and is owned by exactly one user. That user is the
 * only one that is allowed to view or update the Workspace.
 * </p>
 *
 * <p>
 * A Workspace is intended to be a short-lived entity. It is created when a user wants to
 * update an offline flow. Once the user is finished updating the offline flow, the user
 * is expected to publish the new version to the C2 Server / Flow Registry and then the
 * Workspace is expected to be deleted. If the same user or another user later wants to modify
 * the flow again, it is expected that another Workspace will be created. Typically, the user
 * will create a workspace, update it, and publish the offline flow all within a single "session"
 * at the computer. As such, Workspaces are not shared between the nodes in a cluster.
 * </p>
 *
 * <p>
 * When a Workspace is created, it will be created for a specific user and for a specific
 * Command & Control Server and a specific Label within that C2 Server. Other users will not
 * be able to view or modify a Workspace that he/she does not own. In order to avoid merge
 * conflicts, only a single Workspace may exist for a given Label at a time for any particular
 * instance of NiFi.
 * </p>
 *
 * <p>
 * We do store the workspace information on the server, not in a client browser because we do not
 * want to store sensitive information such as passwords on the client using Local Storage. We cannot
 * simply leave it 'in memory' in the client because if the browser crashed or was closed, or the client
 * was restarted, the entire flow would be lost. On the server side, we also persist the flow so that if
 * NiFi is restarted, we will not lose the flow. Additionally, this provides us the opportunity to store
 * larger amounts of information (such as multiple 'revisions' of an OfflineFlow) without having to store
 * it all in the JVM heap.
 * </p>
 */
public interface Workspace {

    /**
     * @return a unique identifier for the workspace
     */
    String getIdentifier();

    /**
     * @return the username of the user who owns this Workspace
     */
    String getOwner();

    /**
     * @return the Label that is associated with this Workspace. This will map directly to a label in the C2 Server.
     */
    String getLabel();

    /**
     * @return the FlowRegistry that is associated with this Workspace
     */
    FlowRegistry getFlowRegistry();

    /**
     * @return the C2 Server that is associated with this Workspace
     */
    C2Server getC2Server();

    /**
     * @return the Offline Flow that is associated with this Workspace
     */
    OfflineFlow getFlow();
}
