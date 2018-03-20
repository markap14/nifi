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
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

public class FileSystemOfflineFlow implements OfflineFlow {
    private final File directory;
    private final OfflineFlowDeserializer deserializer;
    private final int currentRevision;
    private final int minRevision;
    private final int maxRevision;

    public FileSystemOfflineFlow(final File directory, final OfflineFlowDeserializer deserializer, final int currentRevision, final int minRevision, final int maxRevision) {
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
        if (revision < getMinAvailableRevision()) {
            throw new IllegalArgumentException("Cannot retrieve revision " + revision + " of the flow because the minimum available revision is " + getMinAvailableRevision());
        }
        if (revision > getMaxRevision()) {
            throw new IllegalArgumentException("Cannot retrieve revision " + revision + " of the flow because the maximum revision is " + getMaxRevision());
        }

        final File file = getFlowFile(revision);
        try (final InputStream fis = new FileInputStream(file)) {
            deserializer.deserialize(fis);
        }

        return null;
    }

    private File getFlowFile(final int revision) {
        return new File(directory, "flow." + revision);
    }
}
