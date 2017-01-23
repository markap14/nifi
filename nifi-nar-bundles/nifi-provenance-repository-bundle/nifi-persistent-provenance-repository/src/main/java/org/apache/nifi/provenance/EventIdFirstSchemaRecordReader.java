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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.serialization.CompressableRecordReader;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordReader;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;

public class EventIdFirstSchemaRecordReader extends CompressableRecordReader {
    private RecordSchema schema; // effectively final
    private SchemaRecordReader recordReader;  // effectively final

    private List<String> componentIds;
    private List<String> componentTypes;
    private List<String> queueIds;
    private List<String> eventTypes;
    private long firstEventId;
    private long systemTimeOffset;

    public EventIdFirstSchemaRecordReader(final InputStream in, final String filename, final TocReader tocReader, final int maxAttributeChars) throws IOException {
        super(in, filename, tocReader, maxAttributeChars);
    }

    private void verifySerializationVersion(final int serializationVersion) {
        if (serializationVersion > EventIdFirstSchemaRecordWriter.SERIALIZATION_VERSION) {
            throw new IllegalArgumentException("Unable to deserialize record because the version is " + serializationVersion
                + " and supported versions are 1-" + EventIdFirstSchemaRecordWriter.SERIALIZATION_VERSION);
        }
    }

    @Override
    protected synchronized void readHeader(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);
        final int schemaLength = in.readInt();
        final byte[] buffer = new byte[schemaLength];
        StreamUtils.fillBuffer(in, buffer);

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(buffer)) {
            schema = RecordSchema.readFrom(bais);
        }

        recordReader = SchemaRecordReader.fromSchema(schema);

        componentIds = readLookups(in);
        componentTypes = readLookups(in);
        queueIds = readLookups(in);
        eventTypes = readLookups(in);
        firstEventId = in.readLong();
        systemTimeOffset = in.readLong();
    }

    private List<String> readLookups(final DataInputStream in) throws IOException {
        final int listSize = in.readInt();
        final List<String> values = new ArrayList<>(listSize);
        for (int i = 0; i < listSize; i++) {
            values.add(in.readUTF());
        }

        return values;
    }

    @Override
    protected StandardProvenanceEventRecord nextRecord(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);

        final long byteOffset = getBytesConsumed();
        final long eventId = in.readInt() + firstEventId;
        final int recordLength = in.readInt();

        return readRecord(in, eventId, byteOffset, recordLength);
    }

    private StandardProvenanceEventRecord readRecord(final DataInputStream in, final long eventId, final long startOffset, final int recordLength) throws IOException {
        final InputStream limitedIn = new LimitingInputStream(in, recordLength);

        final Record eventRecord = recordReader.readRecord(limitedIn);
        if (eventRecord == null) {
            return null;
        }

        final StandardProvenanceEventRecord deserializedEvent = LookupTableEventRecord.getEvent(eventRecord, getFilename(), startOffset, getMaxAttributeLength(),
            firstEventId, systemTimeOffset, componentIds, componentTypes, queueIds, eventTypes);
        deserializedEvent.setEventId(eventId);
        return deserializedEvent;
    }

    private boolean isData(final InputStream in) throws IOException {
        in.mark(1);
        final int nextByte = in.read();
        in.reset();

        return nextByte > -1;
    }

    @Override
    protected Optional<StandardProvenanceEventRecord> readToEvent(final long eventId, final DataInputStream dis, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);

        while (isData(dis)) {
            final long startOffset = getBytesConsumed();
            final long id = dis.readInt() + firstEventId;
            final int recordLength = dis.readInt();

            if (id >= eventId) {
                final StandardProvenanceEventRecord event = readRecord(dis, id, startOffset, recordLength);
                return Optional.ofNullable(event);
            } else {
                // This is not the record we want. Skip over it instead of deserializing it.
                StreamUtils.skip(dis, recordLength);
            }
        }

        return Optional.empty();
    }
}
