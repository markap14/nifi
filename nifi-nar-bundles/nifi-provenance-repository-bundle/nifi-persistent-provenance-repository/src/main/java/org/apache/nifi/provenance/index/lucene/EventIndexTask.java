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
    public static final int DEFAULT_MAX_EVENTS_PER_COMMIT = 500_000;

    private final BlockingQueue<IndexableDocument> documentQueue;
    private final IndexManager indexManager;
    private volatile boolean shutdown = false;

    private final IndexDirectoryManager directoryManager;
    private final EventReporter eventReporter;
    private final int commitThreshold;

    public EventIndexTask(final BlockingQueue<IndexableDocument> documentQueue, final RepositoryConfiguration repoConfig, final IndexManager indexManager,
        final IndexDirectoryManager directoryManager, final int maxEventPerCommit, final EventReporter eventReporter) {
        this.documentQueue = documentQueue;
        this.indexManager = indexManager;
        this.directoryManager = directoryManager;
        this.commitThreshold = maxEventPerCommit;
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

                // Write documents to the currently active index. If we fail to write the documents, then
                // we will re-queue all of the documents into the error queue.
                // We use a separate error queue instead of adding the events back to the documentQueue because
                // if we use the document queue, we could have a condition where no thread is able to index and
                // attempts to add documents back to the document queue. However, if the document queue is full,
                // then all threads would be blocked trying to add the document back and so nothing would ever
                // get indexed, which would result in a deadlock. Instead, we use a errorQueue that is local
                // to only this "task." I.e., it is not shared by other threads. And since we always pull from
                // the error queue first, we know that we can push events back onto the error queue if we fail
                // to index the events.
                index(toIndex, false, false);
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

    void index(final List<IndexableDocument> toIndex, final boolean forceCommit, final boolean removeDocsInRange) throws IOException {
        if (toIndex.isEmpty()) {
            return;
        }

        final Map<File, List<IndexableDocument>> docsByIndexDir = toIndex.stream().collect(Collectors.groupingBy(doc -> getIndexDirectory(doc)));
        for (final Map.Entry<File, List<IndexableDocument>> entry : docsByIndexDir.entrySet()) {
            final File indexingDirectory = entry.getKey();
            final List<IndexableDocument> documentsForIndex = entry.getValue();
            index(documentsForIndex, indexingDirectory, forceCommit, removeDocsInRange);
        }
    }

    private void index(final List<IndexableDocument> toIndex, final File indexDirectory, final boolean forceCommit, final boolean removeDocsInRange) throws IOException {
        if (toIndex.isEmpty()) {
            return;
        }

        // Convert the IndexableDocument list into a List of Documents so that we can pass them to the Index Writer.
        final List<Document> documents = toIndex.stream()
            .map(doc -> doc.getDocument())
            .collect(Collectors.toList());

        boolean closeIndexWriter = false;
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
            final boolean commitIndex = indexWriter.index(documents, commitThreshold);

            if (forceCommit || commitIndex) {
                commit(indexWriter);
                closeIndexWriter = directoryManager.onIndexCommitted(indexDirectory);
                logger.debug("Committed index {}", indexDirectory);
            }
        } finally {
            indexManager.returnIndexWriter(indexWriter, false, closeIndexWriter);
        }
    }


    protected void commit(final EventIndexWriter indexWriter) throws IOException {
        final long start = System.nanoTime();
        indexWriter.commit();
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Successfully committed Index Writer {} in {} millis", indexWriter, millis);
    }
}
