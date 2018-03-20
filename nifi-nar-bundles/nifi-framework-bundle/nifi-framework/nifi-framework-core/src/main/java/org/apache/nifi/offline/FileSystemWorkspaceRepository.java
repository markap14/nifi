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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.offline.exception.NoSuchWorkspaceException;
import org.apache.nifi.offline.exception.WorkspaceAlreadyExistsException;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemWorkspaceRepository implements WorkspaceRepository {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemWorkspaceRepository.class);
    private static final String FLOW_DIRECTORY_NAME = "flow";
    private static final String FLOW_FILE_EXTENSION = ".xml.gz";
    private static final Pattern UUID_PATTERN = Pattern.compile("[a-f0-9\\\\-]{36}");

    private final WorkspaceSerializer serializer;
    private final WorkspaceDeserializer deserializer;
    private final File storageDir;
    private final OfflineFlowSerializer flowSerializer;
    private final OfflineFlowDeserializer flowDeserializer;
    private final Map<String, Workspace> workspaces = new HashMap<>();

    public FileSystemWorkspaceRepository(final WorkspaceSerializer serializer, final WorkspaceDeserializer deserializer, final File storageDir,
            final OfflineFlowSerializer flowSerializer, final OfflineFlowDeserializer flowDeserializer) throws IOException {

        this.serializer = serializer;
        this.deserializer = deserializer;
        this.storageDir = storageDir;
        this.flowSerializer = flowSerializer;
        this.flowDeserializer = flowDeserializer;

        if (!storageDir.exists() && !storageDir.mkdirs()) {
            throw new IOException("Cannot create Workspace Storage directory " + storageDir);
        }
    }

    @Override
    public synchronized void initialize() throws IOException {
        final File[] files = storageDir.listFiles();
        if (files == null) {
            throw new IOException("Could not obtain listing of files in the storage directory " + storageDir);
        }

        for (final File file : files) {
            if (!file.isDirectory()) {
                continue;
            }

            if (UUID_PATTERN.matcher(file.getName()).matches()) {
                final String uuid = file.getName();
                final Workspace workspace = deserializer.getWorkspace(file.getAbsolutePath());
                workspaces.put(uuid, workspace);
            }
        }
    }

    @Override
    public synchronized Set<Workspace> getWorkspaces() {
        return new HashSet<>(workspaces.values());
    }

    @Override
    public synchronized Workspace createWorkspace(final String owner, final String label, final C2Server c2Server, final FlowRegistry flowRegistry)
        throws IOException, WorkspaceAlreadyExistsException {

        final boolean alreadyExists = workspaces.values().stream()
            .filter(workspace -> workspace.getC2Server().equals(c2Server))
            .anyMatch(workspace -> workspace.getLabel().equals(label));

        if (alreadyExists) {
            // TODO: Are we sticking with the word 'label' here?
            throw new WorkspaceAlreadyExistsException("A Workspace already exists for the Command & Control Server " + c2Server + " and label " + label);
        }

        final String workspaceId = UUID.randomUUID().toString();
        final File workspaceDirectory = getWorkspaceDirectory(workspaceId);

        final File flowDirectory = new File(workspaceDirectory, FLOW_DIRECTORY_NAME);
        final OfflineFlow offlineFlow = new FileSystemOfflineFlow(flowDirectory, flowDeserializer, 0, 0, 0);

        final Workspace workspace = new StandardWorkspace(workspaceId, owner, label, flowRegistry, c2Server, offlineFlow);
        save(workspace);

        logger.debug("Created workspace with ID {}", workspaceId);
        return workspace;
    }

    @Override
    public synchronized Workspace getWorkspace(final String identifier, final String user) throws IOException, NoSuchWorkspaceException {
        final Workspace workspace = workspaces.get(identifier);
        if (workspace == null) {
            throw new NoSuchWorkspaceException("No workspace exists with identifier " + identifier);
        }

        final File workspaceDirectory = getWorkspaceDirectory(identifier);
        if (!workspaceDirectory.exists()) {
            throw new NoSuchWorkspaceException("No workspace exists with identifier " + identifier);
        }

        final String workspaceOwner = workspace.getOwner();
        if (!workspaceOwner.equals(user)) {
            throw new AccessDeniedException();
        }

        return workspace;
    }

    @Override
    public synchronized void deleteWorkspace(final String identifier, final String user) throws IOException, NoSuchWorkspaceException {
        // use getWorkspace to ensure that the workspace exists and that the user is the proper owner
        getWorkspace(identifier, user);

        final File workspaceDirectory = getWorkspaceDirectory(identifier);
        FileUtils.deleteDirectory(workspaceDirectory);

        workspaces.remove(identifier);
        logger.debug("Deleted workspace with ID {}", identifier);
    }

    @Override
    public synchronized Workspace saveWorkspace(final Workspace workspace, final VersionedProcessGroup updatedFlow, final String user) throws IOException, NoSuchWorkspaceException {
        // call getWorkspace to verify that the workspace exists and that the user is the owner.
        getWorkspace(workspace.getIdentifier(), user);

        final String workspaceId = workspace.getIdentifier();
        final File workspaceDirectory = getWorkspaceDirectory(workspaceId);

        final File flowDirectory = new File(workspaceDirectory, FLOW_DIRECTORY_NAME);
        final OfflineFlow currentOfflineFlow = workspace.getFlow();

        final int currentRevision = currentOfflineFlow.getCurrentRevision();
        final int newRevision = currentRevision + 1;
        final File newRevisionFile = new File(flowDirectory, newRevision + FLOW_FILE_EXTENSION);

        // If there are any versions later than the new version, then those must be deleted first.
        final File[] flows = flowDirectory.listFiles();
        for (final File flow : flows) {
            // Filter out any files that are not flows
            final String fileName = flow.getName();
            final int extensionIndex = fileName.indexOf(FLOW_FILE_EXTENSION);
            if (extensionIndex < 1) {
                continue;
            }

            final int revision;
            try {
                revision = Integer.parseInt(fileName.substring(0, extensionIndex));
            } catch (final NumberFormatException nfe) {
                continue;
            }

            if (revision > newRevision) {
                logger.debug("Deleting Offline Flow revision {} at {} because new flow is being saved as revision {}", revision, flow, newRevision);
                Files.delete(flow.toPath());
            }
        }

        // Write the new version of the flow to disk
        try (final OutputStream fos = new FileOutputStream(newRevisionFile);
            final OutputStream gzipOut = new GZIPOutputStream(fos, 1)) {

            flowSerializer.serialize(updatedFlow, gzipOut);
        }

        // Create the new workspace that is updated
        final OfflineFlow updatedOfflineFlow = new FileSystemOfflineFlow(flowDirectory, flowDeserializer, newRevision, 0, newRevision);
        final Workspace updatedWorkspace = new StandardWorkspace(workspace.getIdentifier(), workspace.getOwner(), workspace.getLabel(), workspace.getFlowRegistry(),
            workspace.getC2Server(), updatedOfflineFlow);

        // Save the updated workspace and return it
        save(updatedWorkspace);

        logger.info("Saved new version of Offline Flow with revision {} at {}", newRevision, newRevisionFile);
        return updatedWorkspace;
    }

    @Override
    public synchronized Workspace saveWorkspace(final Workspace workspace, final int flowRevision, final String user) throws IOException, NoSuchWorkspaceException {
        // call getWorkspace to verify that the workspace exists and that the user is the owner.
        final Workspace existingWorkspace = getWorkspace(workspace.getIdentifier(), user);

        final int maxRevision = existingWorkspace.getFlow().getMaxRevision();
        if (flowRevision > maxRevision) {
            throw new IllegalArgumentException("Cannot update Workspace with ID " + workspace.getIdentifier() + " to revision " + flowRevision
                + " because the max revision that exists for this workspace is " + maxRevision);
        }

        final int minRevision = existingWorkspace.getFlow().getMinAvailableRevision();
        if (flowRevision < minRevision) {
            throw new IllegalArgumentException("Cannot update Workspace with ID " + workspace.getIdentifier() + " to revision " + flowRevision
                + " because the minimum revision that exists for this workspace is " + minRevision);
        }

        final File workspaceDirectory = getWorkspaceDirectory(workspace.getIdentifier());
        final File flowDirectory = new File(workspaceDirectory, FLOW_DIRECTORY_NAME);
        final OfflineFlow updatedOfflineFlow = new FileSystemOfflineFlow(flowDirectory, flowDeserializer, flowRevision, minRevision, maxRevision);
        final Workspace updatedWorkspace = new StandardWorkspace(workspace.getIdentifier(), workspace.getOwner(), workspace.getLabel(),
            workspace.getFlowRegistry(), workspace.getC2Server(), updatedOfflineFlow);

        save(updatedWorkspace);
        return updatedWorkspace;
    }

    private void save(final Workspace workspace) throws IOException {
        final File workspaceDirectory = getWorkspaceDirectory(workspace.getIdentifier());
        final String storageLocation = workspaceDirectory.getAbsolutePath();
        serializer.serialize(workspace, storageLocation);

        // Remove the workspace from the set, if it exists, so that we can
        // add it in. If we don't remove first, and a workspace already exists
        // in the set with the same equality, then we will end up not adding the
        // latest version to the workspaces set
        workspaces.put(workspace.getIdentifier(), workspace);
    }

    private File getWorkspaceDirectory(final String identifier) {
        return new File(storageDir, identifier);
    }
}
