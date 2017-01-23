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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.UserEventAuthorizer;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.index.lucene.LuceneEventIndex;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.lucene.SimpleIndexManager;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.store.EventStore;
import org.apache.nifi.provenance.store.PartitionedWriteAheadEventStore;
import org.apache.nifi.provenance.store.RecordReaderFactory;
import org.apache.nifi.provenance.store.RecordWriterFactory;
import org.apache.nifi.provenance.store.StorageResult;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.provenance.util.CloseableUtil;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadProvenanceRepository implements ProvenanceRepository {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadProvenanceRepository.class);
    private static final int BLOCK_SIZE = 1024 * 32;
    public static final String EVENT_CATEGORY = "Provenance Repository";

    private final RepositoryConfiguration config;

    // effectively final
    private EventStore eventStore;
    private EventIndex eventIndex;
    private EventReporter eventReporter;
    private Authorizer authorizer;
    private ProvenanceAuthorizableFactory resourceFactory;

    /**
     * This constructor exists solely for the use of the Java Service Loader mechanism and should not be used.
     */
    public WriteAheadProvenanceRepository() {
        config = null;
    }

    public WriteAheadProvenanceRepository(final NiFiProperties nifiProperties) {
        this(RepositoryConfiguration.create(nifiProperties));
    }

    public WriteAheadProvenanceRepository(final RepositoryConfiguration config) {
        this.config = config;
    }

    @Override
    public synchronized void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory,
        final IdentifierLookup idLookup) throws IOException {
        final RecordWriterFactory recordWriterFactory = (file, idGenerator, compressed, createToc) -> {
            final TocWriter tocWriter = createToc ? new StandardTocWriter(TocUtil.getTocFile(file), false, false) : null;
            return new EventIdFirstSchemaRecordWriter(file, idGenerator, tocWriter, compressed, BLOCK_SIZE, idLookup);
        };

        final RecordReaderFactory recordReaderFactory = (file, logs, maxChars) -> RecordReaders.newRecordReader(file, logs, maxChars);
        eventStore = new PartitionedWriteAheadEventStore(config, recordWriterFactory, recordReaderFactory, eventReporter);

        final IndexManager indexManager = new SimpleIndexManager(config);
        eventIndex = new LuceneEventIndex(config, indexManager, eventReporter);

        this.eventReporter = eventReporter;
        this.authorizer = authorizer;
        this.resourceFactory = resourceFactory;

        eventStore.initialize();
        eventIndex.initialize(eventStore);

        eventStore.reindexLatestEvents(eventIndex);
    }

    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {
        registerEvents(Collections.singleton(event));
    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
        final StorageResult storageResult;

        try {
            storageResult = eventStore.addEvents(events);
        } catch (final IOException e) {
            logger.error("Failed to write events to the Event Store", e);
            eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to write Provenance Events to the repository. See logs for more details.");
            return;
        }

        final Map<ProvenanceEventRecord, StorageSummary> locationMap = storageResult.getStorageLocations();
        if (!locationMap.isEmpty()) {
            eventIndex.addEvents(locationMap);
        }
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        return eventStore.getEvents(firstRecordId, maxRecords);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) throws IOException {
        return eventStore.getEvent(id).orElse(null);
    }

    @Override
    public Long getMaxEventId() {
        return eventStore.getMaxEventId();
    }

    @Override
    public void close() {
        CloseableUtil.closeQuietly(eventStore, eventIndex);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) throws IOException {
        final ProvenanceEventRecord event = getEvent(id);
        if (event == null) {
            return null;
        }

        authorize(event, user);
        return event;
    }

    private void authorize(final ProvenanceEventRecord event, final NiFiUser user) {
        if (authorizer == null) {
            return;
        }

        final Authorizable eventAuthorizable;
        if (event.isRemotePortType()) {
            eventAuthorizable = resourceFactory.createRemoteDataAuthorizable(event.getComponentId());
        } else {
            eventAuthorizable = resourceFactory.createLocalDataAuthorizable(event.getComponentId());
        }
        eventAuthorizable.authorize(authorizer, RequestAction.READ, user, event.getAttributes());
    }


    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords, final NiFiUser user) throws IOException {
        final List<ProvenanceEventRecord> events = getEvents(firstRecordId, maxRecords);
        return createEventAuthorizer(user).filterUnauthorizedEvents(events);
    }

    private EventAuthorizer createEventAuthorizer(final NiFiUser user) {
        return new UserEventAuthorizer(authorizer, resourceFactory, user);
    }

    @Override
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }

    @Override
    public QuerySubmission submitQuery(final Query query, final NiFiUser user) {
        return eventIndex.submitQuery(query, createEventAuthorizer(user), user.getIdentity());
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser user) {
        return eventIndex.retrieveQuerySubmission(queryIdentifier, user);
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final String flowFileUuid, final NiFiUser user) {
        return eventIndex.submitLineageComputation(flowFileUuid, user, createEventAuthorizer(user));
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user) {
        return eventIndex.submitLineageComputation(eventId, user, createEventAuthorizer(user));
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(final String lineageIdentifier, final NiFiUser user) {
        return eventIndex.retrieveLineageSubmission(lineageIdentifier, user);
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        return eventIndex.submitExpandParents(eventId, user, createEventAuthorizer(user));
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        return eventIndex.submitExpandChildren(eventId, user, createEventAuthorizer(user));
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return Collections.unmodifiableList(config.getSearchableFields());
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return Collections.unmodifiableList(config.getSearchableAttributes());
    }
}
