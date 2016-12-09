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

package org.apache.nifi.provenance.lucene;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleIndexManager implements IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(SimpleIndexManager.class);

    private final ConcurrentMap<Object, List<Closeable>> closeables = new ConcurrentHashMap<>();
    private final Map<File, IndexWriterCount> writerCounts = new HashMap<>();

    private final ExecutorService searchExecutor = Executors.newCachedThreadPool();


    @Override
    public void close() throws IOException {
        logger.debug("Shutting down SimpleIndexManager search executor");
        this.searchExecutor.shutdown();
        try {
            if (!this.searchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                this.searchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.searchExecutor.shutdownNow();
        }
    }

    @Override
    public EventIndexSearcher borrowIndexSearcher(final File indexDir) throws IOException {
        logger.debug("Creating index searcher for {}", indexDir);
        final Directory directory = FSDirectory.open(indexDir);
        final DirectoryReader directoryReader = DirectoryReader.open(directory);
        final IndexSearcher searcher = new IndexSearcher(directoryReader, this.searchExecutor);

        final List<Closeable> closeableList = new ArrayList<>(2);
        closeableList.add(directoryReader);
        closeableList.add(directory);
        closeables.put(searcher, closeableList);
        logger.debug("Created index searcher {} for {}", searcher, indexDir);

        return new LuceneEventIndexSearcher(searcher, indexDir, directory, directoryReader);
    }

    @Override
    public void returnIndexSearcher(final EventIndexSearcher searcher) {
        final File indexDirectory = searcher.getIndexDirectory();
        logger.debug("Closing index searcher {} for {}", searcher, indexDirectory);

        final List<Closeable> closeableList = closeables.remove(searcher.getIndexSearcher());
        if (closeableList != null) {
            for (final Closeable closeable : closeableList) {
                closeQuietly(closeable);
            }
        }

        logger.debug("Closed index searcher {}", searcher);
    }

    @Override
    public synchronized boolean removeIndex(final File indexDirectory) {
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.debug("Closing index writer for {}", indexDirectory);

        IndexWriterCount writerCount = writerCounts.remove(absoluteFile);
        if (writerCount == null) {
            return true; // return true since directory has no writers
        }

        if (writerCount.getCount() > 0) {
            writerCounts.put(absoluteFile, writerCount);
            return false;
        }

        try {
            writerCount.close();
        } catch (final Exception e) {
            logger.error("Failed to close Index Writer for {} while removing Index from the repository;"
                + "this directory may need to be cleaned up manually.", e);
        }

        return true;
    }


    @Override
    public synchronized EventIndexWriter borrowIndexWriter(final File indexDirectory) throws IOException {
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.trace("Borrowing index writer for {}", indexDirectory);

        IndexWriterCount writerCount = writerCounts.remove(absoluteFile);
        if (writerCount == null) {
            final List<Closeable> closeables = new ArrayList<>();
            final Directory directory = FSDirectory.open(indexDirectory);
            closeables.add(directory);

            try {
                final Analyzer analyzer = new StandardAnalyzer();
                closeables.add(analyzer);

                final IndexWriterConfig config = new IndexWriterConfig(LuceneUtil.LUCENE_VERSION, analyzer);
                config.setWriteLockTimeout(300000L);

                final ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
                mergeScheduler.setMaxMergesAndThreads(4, 4);
                config.setMergeScheduler(mergeScheduler);

                final IndexWriter indexWriter = new IndexWriter(directory, config);
                final EventIndexWriter eventIndexWriter = new LuceneEventIndexWriter(indexWriter, indexDirectory);

                writerCount = new IndexWriterCount(eventIndexWriter, analyzer, directory, 1);
                logger.debug("Providing new index writer for {}", indexDirectory);
            } catch (final IOException ioe) {
                for (final Closeable closeable : closeables) {
                    try {
                        closeable.close();
                    } catch (final IOException ioe2) {
                        ioe.addSuppressed(ioe2);
                    }
                }

                throw ioe;
            }

            writerCounts.put(absoluteFile, writerCount);
        } else {
            logger.debug("Providing existing index writer for {} and incrementing count to {}", indexDirectory, writerCount.getCount() + 1);
            writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(),
                writerCount.getAnalyzer(), writerCount.getDirectory(), writerCount.getCount() + 1));
        }

        return writerCount.getWriter();
    }

    @Override
    public void returnIndexWriter(final EventIndexWriter writer) {
        returnIndexWriter(writer, true, true);
    }

    @Override
    public synchronized void returnIndexWriter(final EventIndexWriter writer, final boolean commit, final boolean isCloseable) {
        final File indexingDirectory = writer.getDirectory();
        final File absoluteFile = indexingDirectory.getAbsoluteFile();
        logger.trace("Returning Index Writer for {} to IndexManager", indexingDirectory);

        final IndexWriterCount count = writerCounts.get(absoluteFile);

        try {
            if (count == null) {
                logger.warn("Index Writer {} was returned to IndexManager for {}, but this writer is not known. "
                    + "This could potentially lead to a resource leak", writer, indexingDirectory);
                writer.close();
            } else if (count.getCount() <= 1) {
                // we are finished with this writer.
                logger.debug("Decrementing count for Index Writer for {} to {}; Closing writer", indexingDirectory, count.getCount() - 1);
                try {
                    if (commit) {
                        writer.commit();
                    }
                } finally {
                    if (isCloseable) {
                        try {
                            count.close();
                        } finally {
                            writerCounts.remove(absoluteFile);
                        }
                    }
                }
            } else {
                // decrement the count.
                logger.debug("Decrementing count for Index Writer for {} to {}", indexingDirectory, count.getCount() - 1);
                writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(), count.getDirectory(), count.getCount() - 1));
            }
        } catch (final IOException ioe) {
            logger.warn("Failed to close Index Writer {} due to {}", writer, ioe);
            if (logger.isDebugEnabled()) {
                logger.warn("", ioe);
            }
        }
    }

    private static void closeQuietly(final Closeable... closeables) {
        for (final Closeable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                logger.warn("Failed to close {} due to {}", closeable, e);
            }
        }
    }


    private static class IndexWriterCount implements Closeable {
        private final EventIndexWriter writer;
        private final Analyzer analyzer;
        private final Directory directory;
        private final int count;

        public IndexWriterCount(final EventIndexWriter writer, final Analyzer analyzer, final Directory directory, final int count) {
            this.writer = writer;
            this.analyzer = analyzer;
            this.directory = directory;
            this.count = count;
        }

        public Analyzer getAnalyzer() {
            return analyzer;
        }

        public Directory getDirectory() {
            return directory;
        }

        public EventIndexWriter getWriter() {
            return writer;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void close() throws IOException {
            closeQuietly(writer, analyzer, directory);
        }
    }
}
