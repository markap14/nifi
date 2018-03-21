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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.offline.exception.NoSuchWorkspaceException;
import org.apache.nifi.offline.exception.WorkspaceAlreadyExistsException;
import org.apache.nifi.offline.util.InMemoryWorkspaceSerDe;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class TestFileSystemWorkspaceRepository {

    @Rule
    public TemporaryFolder storageDirFactory = new TemporaryFolder();
    private C2Server c2Server;
    private FlowRegistry flowRegistry;
    private File storageDir;

    private InMemoryWorkspaceSerDe workspaceSerde;
    private JaxbOfflineFlowSerDe flowSerde;
    private FileSystemWorkspaceRepository repo;

    @Before
    public void setup() throws IOException {
        c2Server = new C2Server() {
            @Override
            public String getIdentifier() {
                return "unit-test";
            }

            @Override
            public String getURL() {
                return "http://localhost";
            }
        };

        flowRegistry = Mockito.mock(FlowRegistry.class);
        storageDir = storageDirFactory.newFolder();

        workspaceSerde = new InMemoryWorkspaceSerDe();
        flowSerde = new JaxbOfflineFlowSerDe();
        repo = new FileSystemWorkspaceRepository(workspaceSerde, workspaceSerde, storageDir, flowSerde, flowSerde);
    }

    @Test
    public void testCreateWorkspacePersists() throws IOException, WorkspaceAlreadyExistsException, NoSuchWorkspaceException {
        final Workspace workspace = repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);
        assertEquals("owner", workspace.getOwner());
        assertEquals("unit test", workspace.getLabel());
        assertEquals(c2Server, workspace.getC2Server());
        assertEquals(flowRegistry, workspace.getFlowRegistry());

        final Set<String> storageLocations = workspaceSerde.getStorageLocations();
        assertEquals(1, storageLocations.size());

        final Workspace storedWorkspace = workspaceSerde.getWorkspace(storageLocations.iterator().next());
        assertTrue(workspace == storedWorkspace);

        final Workspace repoWorkspace = repo.getWorkspace(workspace.getIdentifier(), "owner");
        assertTrue(repoWorkspace == workspace);
    }

    @Test
    public void testCreateWorkspaceFailsIfWorkspaceExists() throws IOException, WorkspaceAlreadyExistsException {
        repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);

        try {
            repo.createWorkspace("new owner", "unit test", c2Server, flowRegistry);
            fail("Expected to throw WorkspaceAlreadyExistsException but it didn't");
        } catch (final WorkspaceAlreadyExistsException waee) {
            // Expected
        }
    }

    @Test
    public void testDeleteWorkspace() throws IOException, WorkspaceAlreadyExistsException, NoSuchWorkspaceException {
        final Workspace workspace = repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);
        final String workspaceId = workspace.getIdentifier();

        final Workspace retrieved = repo.getWorkspace(workspaceId, "owner");
        assertEquals(workspace, retrieved);

        try {
            repo.deleteWorkspace(workspaceId, "other user");
            fail("Expected AccessDeniedException");
        } catch (final AccessDeniedException ade) {
            // expected
        }

        assertEquals(workspace, repo.getWorkspace(workspaceId, "owner"));

        repo.deleteWorkspace(workspaceId, "owner");

        try {
            repo.getWorkspace(workspaceId, "owner");
            fail("Expected NoSuchWorkspaceException");
        } catch (final NoSuchWorkspaceException nswe) {
            // excpected
        }

        // should now be able to create another workspace for the same label and c2 server
        repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);
    }

    @Test
    public void testSaveWorkspaceWithNewFlow() throws IOException, WorkspaceAlreadyExistsException, NoSuchWorkspaceException {
        final Workspace workspace = repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);

        try {
            repo.saveWorkspace(workspace, new VersionedProcessGroup(), "wrong user", "should fail");
            fail("Expected AccessDeniedException");
        } catch (final AccessDeniedException ade) {
            // expected
        }

        final VersionedProcessGroup pg = new VersionedProcessGroup();
        pg.setName("PG");
        pg.setComments("Unit Test");

        final Workspace updated = repo.saveWorkspace(workspace, pg, "owner", "added PG");
        assertNotSame(workspace, updated);

        final OfflineFlow updatedFlow = updated.getFlow();
        assertEquals(1, updatedFlow.getCurrentRevision());
        assertEquals(0, updatedFlow.getMinAvailableRevision());
        assertEquals(1, updatedFlow.getMaxRevision());
        assertEquals(pg, updatedFlow.getCurrentFlow());
    }

    @Test
    public void testSaveWorkspaceWithDifferentRevision() throws IOException, WorkspaceAlreadyExistsException, NoSuchWorkspaceException {
        final Workspace workspace = repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);

        final VersionedProcessGroup pg = new VersionedProcessGroup();
        pg.setName("PG");
        pg.setComments("Unit Test");

        Workspace updated = workspace;
        for (int i = 0; i < 5; i++) {
            updated = repo.saveWorkspace(updated, pg, "owner", "added PG");
        }

        OfflineFlow flow = updated.getFlow();
        assertEquals(5, flow.getCurrentRevision());
        assertEquals(0, flow.getMinAvailableRevision());
        assertEquals(5, flow.getMaxRevision());

        for (int i = 0; i <= 5; i++) {
            updated = repo.saveWorkspace(updated, i, "owner");
            flow = updated.getFlow();
            assertEquals(i, flow.getCurrentRevision());
            assertEquals(0, flow.getMinAvailableRevision());
            assertEquals(5, flow.getMaxRevision());
        }

        try {
            repo.saveWorkspace(updated, 4, "not the owner");
            fail("Expected AccessDeniedException");
        } catch (final AccessDeniedException ade) {
            // expected
        }

        try {
            repo.saveWorkspace(updated, -1, "owner");
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException ade) {
            // expected
        }

        try {
            repo.saveWorkspace(updated, 6, "owner");
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException ade) {
            // expected
        }

        updated = repo.saveWorkspace(updated, 0, "owner");
        assertNull(updated.getFlow().getCurrentFlow());

        for (int i = 1; i <= 5; i++) {
            updated = repo.saveWorkspace(updated, i, "owner");
            assertEquals(pg, updated.getFlow().getCurrentFlow());
        }
    }

    @Test
    public void testFlowChanges() throws IOException, NoSuchWorkspaceException, WorkspaceAlreadyExistsException {
        final Workspace workspace = repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);

        final VersionedProcessGroup pg = new VersionedProcessGroup();
        pg.setName("PG");
        pg.setComments("Unit Test");

        Workspace updated = workspace;
        for (int i = 1; i <= 5; i++) {
            updated = repo.saveWorkspace(updated, pg, "owner", "Update " + i);
        }

        final OfflineFlow flow = updated.getFlow();

        List<OfflineFlowChange> changes = flow.getChanges(0, 5);
        assertEquals(6, changes.size());

        assertEquals("Flow Created", changes.get(0).getDescription());

        for (int i = 1; i <= 5; i++) {
            assertEquals("Update " + i, changes.get(i).getDescription());
        }
    }

    @Test
    public void testGetChangesWithInvalidIndices() throws IOException, NoSuchWorkspaceException, WorkspaceAlreadyExistsException {
        final Workspace workspace = repo.createWorkspace("owner", "unit test", c2Server, flowRegistry);

        final VersionedProcessGroup pg = new VersionedProcessGroup();
        pg.setName("PG");
        pg.setComments("Unit Test");

        Workspace updated = workspace;
        for (int i = 1; i <= 5; i++) {
            updated = repo.saveWorkspace(updated, pg, "owner", "Update " + i);
        }

        final OfflineFlow flow = updated.getFlow();

        try {
            flow.getChanges(-1, 4);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException iae) {
            // expected
        }

        try {
            flow.getChanges(1, 9);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException iae) {
            // expected
        }

        try {
            flow.getChanges(4, 3);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException iae) {
            // expected
        }

        try {
            flow.getChanges(2, 2);
            fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(expected = NoSuchWorkspaceException.class)
    public void testSaveWorkspaceWithInvalidWorkspaceId() throws IOException, NoSuchWorkspaceException {
        repo.deleteWorkspace("abc", "owner");
    }
}
