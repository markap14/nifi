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

package org.apache.nifi.provenance;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.schema.EventRecord;
import org.apache.nifi.provenance.schema.EventRecordFields;
import org.apache.nifi.provenance.schema.ProvenanceEventSchema;
import org.apache.nifi.provenance.serialization.CompressableRecordWriter;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordWriter;

public class EventIdFirstSchemaRecordWriter extends CompressableRecordWriter {

    private static final RecordSchema eventSchema = ProvenanceEventSchema.PROVENANCE_EVENT_SCHEMA_V1_WITHOUT_EVENT_ID;
    private static final RecordSchema contentClaimSchema = new RecordSchema(eventSchema.getField(EventRecordFields.Names.CONTENT_CLAIM).getSubFields());
    public static final int SERIALIZATION_VERSION = 1;
    public static final String SERIALIZATION_NAME = "EventIdFirstSchemaRecordWriter";

    private final SchemaRecordWriter schemaRecordWriter = new SchemaRecordWriter();
    private final AtomicInteger recordCount = new AtomicInteger(0);

    public EventIdFirstSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize);
    }

    public EventIdFirstSchemaRecordWriter(final OutputStream out, final AtomicLong idGenerator, final TocWriter tocWriter, final boolean compressed,
        final int uncompressedBlockSize) throws IOException {
        super(out, idGenerator, tocWriter, compressed, uncompressedBlockSize);
    }

    @Override
    public StorageSummary writeRecord(final ProvenanceEventRecord record) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        final byte[] serialized;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            final DataOutputStream dos = new DataOutputStream(baos)) {
            writeRecord(record, 0L, dos);
            serialized = baos.toByteArray();
        }

        final long startBytes;
        final long endBytes;
        final long recordIdentifier;
        synchronized (this) {
            try {
                recordIdentifier = record.getEventId() == -1L ? getIdGenerator().getAndIncrement() : record.getEventId();
                startBytes = getBytesWritten();

                ensureStreamState(recordIdentifier, startBytes);
                final DataOutputStream out = getBufferedOutputStream();
                out.writeLong(recordIdentifier);
                out.writeInt(serialized.length);
                out.write(serialized);

                recordCount.incrementAndGet();
                endBytes = getBytesWritten();
            } catch (final IOException ioe) {
                markDirty();
                throw ioe;
            }
        }

        final long serializedLength = endBytes - startBytes;
        final TocWriter tocWriter = getTocWriter();
        final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
        final File file = getFile();
        final String storageLocation = file.getParentFile().getName() + "/" + file.getName();
        return new StorageSummary(recordIdentifier, storageLocation, blockIndex, serializedLength, endBytes);
    }

    @Override
    public int getRecordsWritten() {
        return recordCount.get();
    }

    protected Record createRecord(final ProvenanceEventRecord event, final long eventId) {
        return new EventRecord(event, eventId, eventSchema, contentClaimSchema);
    }

    @Override
    protected void writeRecord(final ProvenanceEventRecord event, final long eventId, final DataOutputStream out) throws IOException {
        final Record eventRecord = createRecord(event, eventId);
        schemaRecordWriter.writeRecord(eventRecord, out);
    }

    @Override
    protected void writeHeader(final long firstEventId, final DataOutputStream out) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        eventSchema.writeTo(baos);

        out.writeInt(baos.size());
        baos.writeTo(out);
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

}
