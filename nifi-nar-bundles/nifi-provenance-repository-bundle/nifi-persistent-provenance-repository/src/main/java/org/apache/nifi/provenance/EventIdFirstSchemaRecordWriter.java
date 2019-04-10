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

import org.apache.nifi.provenance.schema.EventFieldNames;
import org.apache.nifi.provenance.schema.EventIdFirstHeaderSchema;
import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.schema.LookupTableEventSchema;
import org.apache.nifi.provenance.serialization.CompressableRecordWriter;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.provenance.util.ByteArrayDataOutputStream;
import org.apache.nifi.provenance.util.ByteArrayDataOutputStreamCache;
import org.apache.nifi.repository.schema.FieldMapRecord;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordWriter;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class EventIdFirstSchemaRecordWriter extends CompressableRecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(EventIdFirstSchemaRecordWriter.class);

    private static final RecordSchema eventSchema = LookupTableEventSchema.EVENT_SCHEMA;
    private static final RecordSchema contentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.CONTENT_CLAIM).getSubFields());
    private static final RecordSchema previousContentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.PREVIOUS_CONTENT_CLAIM).getSubFields());
    private static final RecordSchema headerSchema = EventIdFirstHeaderSchema.SCHEMA;

    public static final int SERIALIZATION_VERSION = 1;
    public static final String SERIALIZATION_NAME = "EventIdFirstSchemaRecordWriter";
    private final IdentifierLookup idLookup;

    private final SchemaRecordWriter schemaRecordWriter = new SchemaRecordWriter();
    private final AtomicInteger recordCount = new AtomicInteger(0);

    private final Map<String, Integer> componentIdMap;
    private final Map<String, Integer> componentTypeMap;
    private final Map<String, Integer> queueIdMap;
    private static final Map<String, Integer> eventTypeMap;
    private static final List<String> eventTypeNames;

    private static final TimedBuffer<TimestampedLong> serializeTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final TimedBuffer<TimestampedLong> lockTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final TimedBuffer<TimestampedLong> writeTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final TimedBuffer<TimestampedLong> bytesWritten = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final AtomicLong totalRecordCount = new AtomicLong(0L);

    private static final ByteArrayDataOutputStreamCache streamCache = new ByteArrayDataOutputStreamCache(32, 8 * 1024, 256 * 1024);

    private final PhaseManager phaseManager = new PhaseManager();
    private final AtomicLong serializedEventIdGenerator = new AtomicLong(0L);

    private long firstEventId;
    private long systemTimeOffset;

    static {
        eventTypeMap = new HashMap<>();
        eventTypeNames = new ArrayList<>();

        int count = 0;
        for (final ProvenanceEventType eventType : ProvenanceEventType.values()) {
            eventTypeMap.put(eventType.name(), count++);
            eventTypeNames.add(eventType.name());
        }
    }

    public EventIdFirstSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
        final int uncompressedBlockSize, final IdentifierLookup idLookup) throws IOException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize);

        this.idLookup = idLookup;
        componentIdMap = idLookup.invertComponentIdentifiers();
        componentTypeMap = idLookup.invertComponentTypes();
        queueIdMap = idLookup.invertQueueIdentifiers();
    }

    public EventIdFirstSchemaRecordWriter(final OutputStream out, final String storageLocation, final AtomicLong idGenerator, final TocWriter tocWriter, final boolean compressed,
        final int uncompressedBlockSize, final IdentifierLookup idLookup) throws IOException {
        super(out, storageLocation, idGenerator, tocWriter, compressed, uncompressedBlockSize);

        this.idLookup = idLookup;
        componentIdMap = idLookup.invertComponentIdentifiers();
        componentTypeMap = idLookup.invertComponentTypes();
        queueIdMap = idLookup.invertQueueIdentifiers();
    }

    @Override
    public Map<ProvenanceEventRecord, StorageSummary> writeRecords(final Iterable<ProvenanceEventRecord> events) throws IOException {
//        if (true) {
//            return super.writeRecords(events);
//        }

        // TODO: Cleanup of these data structures
        // TODO: Return something like a BlockStorageSummary instead, which is basically a List of StoredProvenanceEvent's, where StoredProvenanceEvent contains getter for event and getter for
        //  StorageSumary. As-is, we are using HashMap's and we always use it to simply iterate over the entrySet. We never do contains() or get() so the HashMap is more expensive than it needs to be.

        final SerializedEvents serializedEvents = serializeEvents(events);
        final EventPhase eventPhase = phaseManager.register(serializedEvents);

        try {
            return eventPhase.get(serializedEvents.getId());
        } catch (final ExecutionException ee) {
            final Throwable cause = ee.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }

            throw new IOException(cause);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
    }

    private SerializedEvents serializeEvents(final Iterable<ProvenanceEventRecord> events) throws IOException {
        final long start = System.nanoTime();

        final Map<ProvenanceEventRecord, byte[]> serializedEvents = new LinkedHashMap<>();
        final ByteArrayDataOutputStream bados = streamCache.checkOut();
        try {
            for (final ProvenanceEventRecord event : events) {
                writeRecord(event, 0L, bados.getDataOutputStream());

                final ByteArrayOutputStream baos = bados.getByteArrayOutputStream();
                final byte[] serializedEvent = baos.toByteArray();
                serializedEvents.put(event, serializedEvent);
                baos.reset();
            }
        } finally {
            streamCache.checkIn(bados);
        }

        final long end = System.nanoTime();
        serializeTimes.add(new TimestampedLong(end - start));

        return new SerializedEvents(serializedEvents, serializedEventIdGenerator.getAndIncrement());
    }


    @Override
    public StorageSummary writeRecord(final ProvenanceEventRecord record) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        final long lockStart;
        final long writeStart;
        final long startBytes;
        final long endBytes;
        final long recordIdentifier;

        final long serializeStart = System.nanoTime();
        final ByteArrayDataOutputStream bados = streamCache.checkOut();
        try {
            writeRecord(record, 0L, bados.getDataOutputStream());

            lockStart = System.nanoTime();
            synchronized (this) {
                writeStart = System.nanoTime();
                try {
                    recordIdentifier = record.getEventId() == -1L ? getIdGenerator().getAndIncrement() : record.getEventId();
                    startBytes = getBytesWritten();

                    ensureStreamState(recordIdentifier, startBytes);

                    final DataOutputStream out = getBufferedOutputStream();
                    final int recordIdOffset = (int) (recordIdentifier - firstEventId);
                    out.writeInt(recordIdOffset);

                    final ByteArrayOutputStream baos = bados.getByteArrayOutputStream();
                    out.writeInt(baos.size());
                    baos.writeTo(out);

                    recordCount.incrementAndGet();
                    endBytes = getBytesWritten();
                } catch (final IOException ioe) {
                    markDirty();
                    throw ioe;
                }
            }
        } finally {
            streamCache.checkIn(bados);
        }

        if (logger.isDebugEnabled()) {
            // Collect stats and periodically dump them if log level is set to at least info.
            final long writeNanos = System.nanoTime() - writeStart;
            writeTimes.add(new TimestampedLong(writeNanos));

            final long serializeNanos = lockStart - serializeStart;
            serializeTimes.add(new TimestampedLong(serializeNanos));

            final long lockNanos = writeStart - lockStart;
            lockTimes.add(new TimestampedLong(lockNanos));
            bytesWritten.add(new TimestampedLong(endBytes - startBytes));

            final long recordCount = totalRecordCount.incrementAndGet();
            if (recordCount % 1_000_000 == 0) {
                final long sixtySecondsAgo = System.currentTimeMillis() - 60000L;
                final Long writeNanosLast60 = writeTimes.getAggregateValue(sixtySecondsAgo).getValue();
                final Long lockNanosLast60 = lockTimes.getAggregateValue(sixtySecondsAgo).getValue();
                final Long serializeNanosLast60 = serializeTimes.getAggregateValue(sixtySecondsAgo).getValue();
                final Long bytesWrittenLast60 = bytesWritten.getAggregateValue(sixtySecondsAgo).getValue();
                logger.debug("In the last 60 seconds, have spent {} millis writing to file ({} MB), {} millis waiting on synchronize block, {} millis serializing events",
                    TimeUnit.NANOSECONDS.toMillis(writeNanosLast60),
                    bytesWrittenLast60 / 1024 / 1024,
                    TimeUnit.NANOSECONDS.toMillis(lockNanosLast60),
                    TimeUnit.NANOSECONDS.toMillis(serializeNanosLast60));
            }
        }

        final long serializedLength = endBytes - startBytes;
        final TocWriter tocWriter = getTocWriter();
        final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
        final File file = getFile();
        final String storageLocation = file == null ? getStorageLocation() : file.getParentFile().getName() + "/" + file.getName();
        return new StorageSummary(recordIdentifier, storageLocation, blockIndex, serializedLength, endBytes);
    }

    @Override
    public int getRecordsWritten() {
        return recordCount.get();
    }

    protected Record createRecord(final ProvenanceEventRecord event, final long eventId) {
        return new LookupTableEventRecord(event, eventId, eventSchema, contentClaimSchema, previousContentClaimSchema, firstEventId, systemTimeOffset,
            componentIdMap, componentTypeMap, queueIdMap, eventTypeMap);
    }

    @Override
    protected void writeRecord(final ProvenanceEventRecord event, final long eventId, final DataOutputStream out) throws IOException {
        final Record eventRecord = createRecord(event, eventId);
        schemaRecordWriter.writeRecord(eventRecord, out);
    }

    @Override
    protected synchronized void writeHeader(final long firstEventId, final DataOutputStream out) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        eventSchema.writeTo(baos);

        out.writeInt(baos.size());
        baos.writeTo(out);

        baos.reset();
        headerSchema.writeTo(baos);
        out.writeInt(baos.size());
        baos.writeTo(out);

        this.firstEventId = firstEventId;
        this.systemTimeOffset = System.currentTimeMillis();

        final Map<String, Object> headerValues = new HashMap<>();
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.FIRST_EVENT_ID, firstEventId);
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.TIMESTAMP_OFFSET, systemTimeOffset);
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.COMPONENT_IDS, idLookup.getComponentIdentifiers());
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.COMPONENT_TYPES, idLookup.getComponentTypes());
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.QUEUE_IDS, idLookup.getQueueIdentifiers());
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.EVENT_TYPES, eventTypeNames);
        final FieldMapRecord headerInfo = new FieldMapRecord(headerSchema, headerValues);

        schemaRecordWriter.writeRecord(headerInfo, out);

        final File file = getFile();
        final String location = file == null ? getStorageLocation() : file.getName();

        final Thread writeThread = new Thread(new WriteEventTask(phaseManager));
        writeThread.setName("Provenance Event Writer - " + location);
        writeThread.start();
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

    /* Getters for internal state written to by subclass EncryptedSchemaRecordWriter */

    IdentifierLookup getIdLookup() {
        return idLookup;
    }

    SchemaRecordWriter getSchemaRecordWriter() {
        return schemaRecordWriter;
    }

    AtomicInteger getRecordCount() {
        return recordCount;
    }

    static TimedBuffer<TimestampedLong> getSerializeTimes() {
        return serializeTimes;
    }

    static TimedBuffer<TimestampedLong> getLockTimes() {
        return lockTimes;
    }

    static TimedBuffer<TimestampedLong> getWriteTimes() {
        return writeTimes;
    }

    static TimedBuffer<TimestampedLong> getBytesWrittenBuffer() {
        return bytesWritten;
    }

    static AtomicLong getTotalRecordCount() {
        return totalRecordCount;
    }

    long getFirstEventId() {
        return firstEventId;
    }

    long getSystemTimeOffset() {
        return systemTimeOffset;
    }


    private static class SerializedEvents {
        private final Map<ProvenanceEventRecord, byte[]> serializedEvents;
        private final long byteCount;
        private final long id;

        public SerializedEvents(final Map<ProvenanceEventRecord, byte[]> serializedEvents, final long id) {
            this.serializedEvents = serializedEvents;

            long bytes = 0L;
            for (final byte[] eventData : serializedEvents.values()) {
                bytes += eventData.length;
            }
            this.byteCount = bytes;
            this.id = id;
        }

        public Map<ProvenanceEventRecord, byte[]> getEvents() {
            return serializedEvents;
        }

        public long getByteCount() {
            return byteCount;
        }

        public long getId() {
            return id;
        }
    }


    // track phases...
    // When we add to the queue, we determine which phase it will be in and then wait for that phase.
    // We determine which phase by considering if the current phase has reached max number of bytes. If so, then we
    // increment phase and set byteCount to 0.
    // If not, then we use the current phase.
    //
    // When we pull from the queue, we first peek to see if the next one will be in the same phase. If not,
    // then we do not accept it.
    // If it is in the current phase, then we accept it, along with any other events in that phase.
    // Then, if queue is empty, we increment the phase number.
    // Then, write all events and advance to the next stage.

    private static class PhaseManager {
        private static final int DESIRED_BYTES_PER_PHASE = 8192;

        private EventPhase currentPhase = new EventPhase();
        private final BlockingQueue<EventPhase> phaseQueue = new LinkedBlockingQueue<>();
        private final ReentrantLock lock = new ReentrantLock();

        public EventPhase register(final SerializedEvents events) {
            lock.lock();
            try {
                final EventPhase phase = currentPhase;
                phase.addEvents(events);

                if (currentPhase.getByteCount() >= DESIRED_BYTES_PER_PHASE || phaseQueue.isEmpty()) {
                    incrementPhase();
                }

                return phase;
            } finally {
                lock.unlock();
            }
        }

        private void incrementPhase() {
            phaseQueue.offer(currentPhase);
            currentPhase = new EventPhase();
        }

        public boolean isEmpty() {
            return phaseQueue.isEmpty() && currentPhase.isEmpty();
        }

        public EventPhase nextPhase() throws InterruptedException {
            final EventPhase initialEventPhase = phaseQueue.poll(10, TimeUnit.MILLISECONDS);
            if (initialEventPhase != null) {
                return initialEventPhase;
            }

            lock.lock();
            try {
                if (phaseQueue.isEmpty() && !currentPhase.isEmpty()) {
                    final EventPhase evictedEventPhase = currentPhase;
                    currentPhase = new EventPhase();
                    return evictedEventPhase;
                }
            } finally {
                lock.unlock();
            }

            return null;
        }
    }



    private class WriteEventTask implements Runnable {
        private final PhaseManager phaseManager;
        private final String location;

        private int eventsWritten = 0;
        private int phasesWritten = 0;
        private long totalBytesWritten = 0L;
        private final long startTime = System.currentTimeMillis();

        public WriteEventTask(final PhaseManager phaseManager) {
            this.phaseManager = phaseManager;

            final File file = getFile();
            location = file == null ? getStorageLocation() : file.getName();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    final EventPhase eventPhase = phaseManager.nextPhase();
                    if (eventPhase == null) {
                        if (isClosed() && phaseManager.isEmpty()) {
                            final long seconds = (System.currentTimeMillis() - startTime) / 1000;
                            final long mbWritten = totalBytesWritten / 1000000;
                            logger.info("Wrote {} events to {} in {} phases (average of {} events/phase), {} MB in {} seconds = {} MB/sec", eventsWritten, location, phasesWritten,
                                (double) eventsWritten / phasesWritten, mbWritten, seconds, mbWritten / seconds);

                            return;
                        }

                        continue;
                    }

                    final Map<Long, Map<ProvenanceEventRecord, StorageSummary>> storageSummaries;
                    try {
                        storageSummaries = writeEvents(eventPhase);
                        eventPhase.complete(storageSummaries);
                        phasesWritten++;
                        eventsWritten += eventPhase.getEvents().size();
                        totalBytesWritten += eventPhase.getByteCount();
                    } catch (final Throwable t) {
                        markDirty();
                        logger.error("Failed to write Provenance Events to {}", location, t);
                        eventPhase.fail(t);

                        completeAllExceptionally();
                    }
                } catch (final Throwable t) {
                    logger.error("Failed to write Provenance Events to {}", location, t);
                }
            }
        }

        private void completeAllExceptionally() throws InterruptedException {
            try {
                close();
            } catch (IOException e) {
                logger.error("Failed to close Provenance Event Writer for " + location);
            }

            final IOException ioe = new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");

            EventPhase phase;
            while ((phase = phaseManager.nextPhase()) != null) {
                phase.fail(ioe);
            }
        }

        private Map<Long, Map<ProvenanceEventRecord, StorageSummary>> writeEvents(final EventPhase eventPhase) throws IOException {
            final long writeStart;

            final TocWriter tocWriter = getTocWriter();
            final File file = getFile();
            final String storageLocation = file == null ? getStorageLocation() : file.getParentFile().getName() + "/" + file.getName();
            long byteCount = 0L;

            final Map<Long, Map<ProvenanceEventRecord, StorageSummary>> allSummaries = new HashMap<>();

            writeStart = System.nanoTime();
            try {
                final List<SerializedEvents> serializedEventsList = eventPhase.getEvents();

                final DataOutputStream out = getBufferedOutputStream();

                for (final SerializedEvents events : serializedEventsList) {
                    final Map<ProvenanceEventRecord, StorageSummary> storageSummaryMap = new HashMap<>();
                    allSummaries.put(events.getId(), storageSummaryMap);

                    for (final Map.Entry<ProvenanceEventRecord, byte[]> entry : events.getEvents().entrySet()) {
                        final ProvenanceEventRecord event = entry.getKey();
                        final byte[] serializedBytes = entry.getValue();

                        final long startBytes = getBytesWritten();
                        final long recordIdentifier = event.getEventId() == -1 ? getIdGenerator().getAndIncrement() : event.getEventId();
                        ensureStreamState(recordIdentifier, startBytes);

                        final int recordIdOffset = (int) (recordIdentifier - firstEventId);
                        out.writeInt(recordIdOffset);

                        out.writeInt(serializedBytes.length);
                        out.write(serializedBytes);

                        recordCount.incrementAndGet();
                        final long endBytes = getBytesWritten();

                        final long serializedLength = endBytes - startBytes;
                        final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
                        final StorageSummary storageSummary = new StorageSummary(recordIdentifier, storageLocation, blockIndex, serializedLength, endBytes);
                        storageSummaryMap.put(event, storageSummary);
                        byteCount += endBytes - startBytes;
                    }
                }

                out.flush();
            } catch (final IOException ioe) {
                markDirty();
                throw ioe;
            }


            if (logger.isDebugEnabled()) {
                // Collect stats and periodically dump them if log level is set to at least info.
                final long writeNanos = System.nanoTime() - writeStart;
                writeTimes.add(new TimestampedLong(writeNanos));
                bytesWritten.add(new TimestampedLong(byteCount));

                final long recordCount = totalRecordCount.incrementAndGet();
                if (recordCount % 1_000_000 == 0) {
                    final long sixtySecondsAgo = System.currentTimeMillis() - 60000L;
                    final Long writeNanosLast60 = writeTimes.getAggregateValue(sixtySecondsAgo).getValue();
                    final Long bytesWrittenLast60 = bytesWritten.getAggregateValue(sixtySecondsAgo).getValue();
                    logger.debug("In the last 60 seconds, have spent {} millis writing to file ({} MB)",
                        TimeUnit.NANOSECONDS.toMillis(writeNanosLast60),
                        bytesWrittenLast60 / 1024 / 1024);
                }
            }

            return allSummaries;
        }
    }


    private static class EventPhase {
        private final CompletableFuture<Map<Long, Map<ProvenanceEventRecord, StorageSummary>>> future = new CompletableFuture<>();
        private final List<SerializedEvents> events = new ArrayList<>();
        private long byteCount = 0L;


        public void addEvents(final SerializedEvents events) {
            this.events.add(events);
            this.byteCount += events.getByteCount();
        }

        public List<SerializedEvents> getEvents() {
            return events;
        }

        public boolean isEmpty() {
            return events.isEmpty();
        }

        public long getByteCount() {
            return byteCount;
        }

        public void complete(final Map<Long, Map<ProvenanceEventRecord, StorageSummary>> storageSummaries) {
            future.complete(storageSummaries);
        }

        public void fail(final Throwable t) {
            future.completeExceptionally(t);
        }

        public Map<ProvenanceEventRecord, StorageSummary> get(final long serializedEventsId) throws ExecutionException, InterruptedException {
            final Map<Long, Map<ProvenanceEventRecord, StorageSummary>> allEvents = future.get();
            return allEvents.get(serializedEventsId);
        }
    }
}
