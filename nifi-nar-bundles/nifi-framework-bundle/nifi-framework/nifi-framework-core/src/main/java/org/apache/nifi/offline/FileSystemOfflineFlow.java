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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemOfflineFlow implements OfflineFlow {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemOfflineFlow.class);

    private static final String FILE_EXTENSION = ".xml.gz";

    private final File directory;
    private final OfflineFlowSerializer serializer;
    private final OfflineFlowDeserializer deserializer;
    private final int currentRevision;
    private final int minRevision;
    private final int maxRevision;

    /**
     * Creates a new FileSystemOfflineFlow that is capable of reading flows from disk but not capable of writing flows to disk
     *
     * @param directory the directory to read from
     * @param deserializer the deserializer to use for parsing the flow
     * @param currentRevision the current revision
     * @param minRevision the minimum revision available
     * @param maxRevision the max revision available
     */
    public FileSystemOfflineFlow(final File directory, final OfflineFlowDeserializer deserializer, final int currentRevision, final int minRevision, final int maxRevision) {
        this(directory, null, deserializer, currentRevision, minRevision, maxRevision);
    }

    /**
     * Creates a new FileSystemOfflineFlow that is capable of both reading flows from disk and writing flows to disk
     *
     * @param directory the directory to read from
     * @param serializer the serializer to use for writing the flow
     * @param deserializer the deserializer to use for parsing the flow
     * @param currentRevision the current revision
     * @param minRevision the minimum revision available
     * @param maxRevision the max revision available
     */
    public FileSystemOfflineFlow(final File directory, final OfflineFlowSerializer serializer, final OfflineFlowDeserializer deserializer,
        final int currentRevision, final int minRevision, final int maxRevision) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.directory = directory;
        this.currentRevision = currentRevision;
        this.minRevision = minRevision;
        this.maxRevision = maxRevision;
    }

    @Override
    public int getCurrentRevision() {
        return currentRevision;
    }

    @Override
    public int getMinAvailableRevision() {
        return minRevision;
    }

    @Override
    public int getMaxRevision() {
        return maxRevision;
    }

    @Override
    public VersionedProcessGroup getFlow(final int revision) throws IOException {
        final OfflineFlowRevision flowRevision = getFlowRevision(revision);
        return flowRevision == null ? null : flowRevision.getFlow();
    }

    private OfflineFlowRevision getFlowRevision(final int revision) throws IOException {
        if (revision < getMinAvailableRevision()) {
            throw new IllegalArgumentException("Cannot retrieve revision " + revision + " of the flow because the minimum available revision is " + getMinAvailableRevision());
        }
        if (revision > getMaxRevision()) {
            throw new IllegalArgumentException("Cannot retrieve revision " + revision + " of the flow because the maximum revision is " + getMaxRevision());
        }

        // Revision 0 is always an empty flow because nothing yet has been saved
        if (revision == 0) {
            return null;
        }

        final File file = getFlowFile(revision);
        try (final InputStream fis = new FileInputStream(file);
            final InputStream gzipIn = new GZIPInputStream(fis)) {
            return deserializer.deserialize(gzipIn);
        }
    }

    /**
     * Writes the given VersionedProcessGroup as the current revision of the flow and deletes any existing revisions
     * that are later than the current revision. This does not increment the revision of the flow.
     *
     * @param flow the new flow
     * @param changeDescription a description of how the flow changed from its last revision
     * @throws IOException if unable to write the flow to disk
     */
    public void writeFlow(final VersionedProcessGroup flow, final String changeDescription) throws IOException {
        if (serializer == null) {
            throw new IllegalStateException("Cannot write flow with this FileSystemOfflineFlow because it was created without specifying an OfflineFlowSerializer");
        }

        final File newRevisionFile = new File(directory, currentRevision + FILE_EXTENSION);

        // Write the new version of the flow to disk
        final OfflineFlowRevision offlineFlowRevision = new OfflineFlowRevision();
        offlineFlowRevision.setDescription(changeDescription);
        offlineFlowRevision.setTimestamp(new Date());
        offlineFlowRevision.setFlow(flow);

        try (final OutputStream fos = new FileOutputStream(newRevisionFile);
            final OutputStream gzipOut = new GZIPOutputStream(fos, 1)) {

            serializer.serialize(offlineFlowRevision, gzipOut);
        }

        // If there are any versions later than the new version, then those must be deleted.
        // The idea here is that we can undo, say 12 steps. Then we can redo 5 steps. This is
        // handled by modifying the workspace's current revision. However, as soon as we make
        // a change to the workspace (i.e., call this method), then we can no longer 'redo'
        // anything because it no longer makes sense. This is the typical undo/redo scenario
        // presented by most applications. So we delete any subsequent revisions.
        final File[] flows = directory.listFiles();
        for (final File flowRevision : flows) {
            // Filter out any files that are not flows
            final String fileName = flowRevision.getName();
            final int extensionIndex = fileName.indexOf(FILE_EXTENSION);
            if (extensionIndex < 1) {
                continue;
            }

            final int revision;
            try {
                revision = Integer.parseInt(fileName.substring(0, extensionIndex));
            } catch (final NumberFormatException nfe) {
                continue;
            }

            if (revision > currentRevision) {
                logger.debug("Deleting Offline Flow revision {} at {} because new flow is being saved as revision {}", revision, flowRevision, currentRevision);
                Files.delete(flowRevision.toPath());
            }
        }
    }

    @Override
    public List<OfflineFlowChange> getChanges(final int fromRevision, final int toRevision) throws IOException {
        if (fromRevision < getMinAvailableRevision()) {
            throw new IllegalArgumentException("Cannot retrieve changes because the supplied revision ("
                + fromRevision + ") is less than the minimum available Revision of " + getMinAvailableRevision());
        }

        if (toRevision > getMaxRevision()) {
            throw new IllegalArgumentException("Cannot retrieve changes because the supplied revision ("
                + toRevision + ") is less than the minimum available Revision of " + getMaxRevision());
        }

        if (fromRevision >= toRevision) {
            throw new IllegalArgumentException("Cannot retrieve changes because the minimum revision must be less than the maximum revision");
        }

        final List<OfflineFlowChange> changes = new ArrayList<>(toRevision - fromRevision);
        for (int i = fromRevision; i <= toRevision; i++) {
            final OfflineFlowChange offlineFlowChange;

            if (i == 0) {
                offlineFlowChange = new StandardOfflineFlowChange(0, new Date(getFlowFile(0).lastModified()), "Flow Created");
            } else {
                final OfflineFlowRevision revision = getFlowRevision(i);
                offlineFlowChange = new StandardOfflineFlowChange(i, revision.getTimestamp(), revision.getDescription());
            }

            changes.add(offlineFlowChange);
        }

        return changes;
    }

    private File getFlowFile(final int revision) {
        return new File(directory, revision + FILE_EXTENSION);
    }
}
