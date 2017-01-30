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

package org.apache.nifi.provenance.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventIndexTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(EventIndexTask.class);
    private static final String EVENT_CATEGORY = "Provenance Repository";
    public static final int MAX_DOCUMENTS_PER_THREAD = 100;
    public static final int DEFAULT_MAX_EVENTS_PER_COMMIT = 1_000_000;

    private final BlockingQueue<IndexableDocument> documentQueue;
    private final IndexManager indexManager;
    private volatile boolean shutdown = false;

    private final IndexDirectoryManager directoryManager;
    private final EventReporter eventReporter;
    private final int commitThreshold;

    public EventIndexTask(final BlockingQueue<IndexableDocument> documentQueue, final RepositoryConfiguration repoConfig, final IndexManager indexManager,
        final IndexDirectoryManager directoryManager, final int maxEventsPerCommit, final EventReporter eventReporter) {
        this.documentQueue = documentQueue;
        this.indexManager = indexManager;
        this.directoryManager = directoryManager;
        this.commitThreshold = maxEventsPerCommit;
        this.eventReporter = eventReporter;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    private void fetchDocuments(final List<IndexableDocument> destination) throws InterruptedException {
        // We want to fetch up to INDEX_BUFFER_SIZE documents at a time. However, we don't want to continually
        // call #drainTo on the queue. So we call poll, blocking for up to 1 second. If we get any event, then
        // we will call drainTo to gather the rest. If we get no events, then we just return, having gathered
        // no events.
        IndexableDocument firstDoc = documentQueue.poll(1, TimeUnit.SECONDS);
        if (firstDoc == null) {
            return;
        }

        destination.add(firstDoc);
        documentQueue.drainTo(destination, MAX_DOCUMENTS_PER_THREAD - 1);
    }

    @Override
    public void run() {
        final List<IndexableDocument> toIndex = new ArrayList<>(MAX_DOCUMENTS_PER_THREAD);

        while (!shutdown) {
            try {
                // Get the Documents that we want to index.
                toIndex.clear();
                fetchDocuments(toIndex);

                if (toIndex.isEmpty()) {
                    continue;
                }

                // Write documents to the currently active index.
                index(toIndex, CommitPreference.NO_PREFERENCE, false);
            } catch (final Exception e) {
                logger.error("Failed to index Provenance Events", e);
                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to index Provenance Events. See logs for more information.");
            }
        }
    }


    private File getIndexDirectory(final IndexableDocument doc) {
        final Optional<File> specifiedIndexDir = doc.getIndexDirectory();
        if (specifiedIndexDir.isPresent()) {
            return specifiedIndexDir.get();
        } else {
            final long firstTimestamp = doc.getDocument().getField(SearchableFields.EventTime.getSearchableFieldName()).numericValue().longValue();
            final String partitionName = doc.getPersistenceLocation().getPartitionName()
                .orElseThrow(() -> new IllegalArgumentException("IndexableDocument must contain either a Partition Name or an Index Directory"));
            return directoryManager.getWritableIndexingDirectory(firstTimestamp, partitionName);
        }
    }

    void index(final List<IndexableDocument> toIndex, final CommitPreference commitPreference, final boolean removeDocsInRange) throws IOException {
        if (toIndex.isEmpty()) {
            return;
        }

        final Map<File, List<IndexableDocument>> docsByIndexDir = toIndex.stream().collect(Collectors.groupingBy(doc -> getIndexDirectory(doc)));
        for (final Map.Entry<File, List<IndexableDocument>> entry : docsByIndexDir.entrySet()) {
            final File indexingDirectory = entry.getKey();
            final List<IndexableDocument> documentsForIndex = entry.getValue();
            index(documentsForIndex, indexingDirectory, commitPreference, removeDocsInRange);
        }
    }

    private void index(final List<IndexableDocument> toIndex, final File indexDirectory, final CommitPreference commitPreference, final boolean removeDocsInRange) throws IOException {
        if (toIndex.isEmpty()) {
            return;
        }

        // Convert the IndexableDocument list into a List of Documents so that we can pass them to the Index Writer.
        final List<Document> documents = toIndex.stream()
            .map(doc -> doc.getDocument())
            .collect(Collectors.toList());

        boolean closeIndexWriter = false;
        boolean requestCommit = false;
        final EventIndexWriter indexWriter = indexManager.borrowIndexWriter(indexDirectory);
        try {
            // If removeDocsInRange is true, we need to remove all documents in the Lucene Index that are between the min and max
            // event ID's given. This is done, for instance, to re-index the events when we don't know whether or not the events
            // are already contained in the index.
            if (removeDocsInRange) {
                long minId = Long.MAX_VALUE;
                long maxId = Long.MIN_VALUE;

                for (final IndexableDocument doc : toIndex) {
                    final long eventId = doc.getDocument().getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue();
                    if (eventId < minId) {
                        minId = eventId;
                    }
                    if (eventId > maxId) {
                        maxId = eventId;
                    }
                }

                final NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange(
                    SearchableFields.Identifier.getSearchableFieldName(), minId, maxId, true, true);
                indexWriter.getIndexWriter().deleteDocuments(query);
            }

            // Perform the actual indexing.
            boolean writerIndicatesCommit = indexWriter.index(documents, commitThreshold);

            final boolean commit;
            if (CommitPreference.FORCE_COMMIT == commitPreference) {
                commit = true;
            } else if (CommitPreference.PREVENT_COMMIT == commitPreference) {
                commit = false;
            } else {
                // If we don't need to commit index based on what index writer tells us, we will still want
                // to commit the index if it's assigned to a partition and this is no longer the active index
                // for that partition. This prevents the following case:
                //
                // Thread T1: pulls events from queue
                //            Maps events to Index Directory D1
                // Thread T2: pulls events from queue
                //            Maps events to Index Directory D1, the active index for Partition P1.
                //            Writes events to D1.
                //            Commits Index Writer for D1.
                //            Closes Index Writer for D1.
                // Thread T1: Writes events to D1.
                //            Determines that Index Writer for D1 does not need to be committed or closed.
                //
                // In the case outlined above, we would potentially lose those events from the index! To avoid this,
                // we simply decide to commit the index if this writer is no longer the active writer for the index.
                // However, if we have 10 threads, we don't want all 10 threads trying to commit the index after each
                // update. We want to commit when they've all finished. This is what the IndexManager will do if we request
                // that it commit the index. It will also close the index if requested, once all writers have finished.
                // So when this is the case, we will request that the Index Manager both commit and close the writer.
                final Optional<String> partitionNameOption = toIndex.get(0).getPersistenceLocation().getPartitionName();
                if (partitionNameOption.isPresent()) {
                    final String partitionName = partitionNameOption.get();
                    final Optional<File> activeIndexDirOption = directoryManager.getActiveIndexDirectory(partitionName);
                    if (activeIndexDirOption.isPresent() && !activeIndexDirOption.get().equals(indexDirectory)) {
                        requestCommit = true;
                        closeIndexWriter = true;
                    }
                }

                commit = writerIndicatesCommit;
            }

            if (commit) {
                commit(indexWriter);
                requestCommit = false; // we've already committed the index writer so no need to request that the index manager do so also.
                final boolean directoryManagerIndicatesClose = directoryManager.onIndexCommitted(indexDirectory);
                closeIndexWriter = closeIndexWriter || directoryManagerIndicatesClose;

                if (logger.isDebugEnabled()) {
                    final long maxId = documents.stream()
                        .mapToLong(doc -> doc.getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue())
                        .max()
                        .orElse(-1L);
                    logger.debug("Committed index {} after writing a max Event ID of {}", indexDirectory, maxId);
                }
            }
        } finally {
            indexManager.returnIndexWriter(indexWriter, requestCommit, closeIndexWriter);
        }
    }


    protected void commit(final EventIndexWriter indexWriter) throws IOException {
        final long start = System.nanoTime();
        final long approximateCommitCount = indexWriter.commit();
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Successfully committed approximately {} Events to {} in {} millis", approximateCommitCount, indexWriter, millis);
    }
}
