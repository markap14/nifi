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
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexDirectoryManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexDirectoryManager.class);
    private static final FileFilter INDEX_DIRECTORY_FILTER = f -> f.getName().startsWith("index-");
    private static final Pattern INDEX_FILENAME_PATTERN = Pattern.compile("index-(\\d+)");

    private final RepositoryConfiguration repoConfig;

    private final SortedMap<Long, List<IndexLocation>> indexLocationByTimestamp = new TreeMap<>();
    private final Map<String, IndexLocation> activeIndices = new HashMap<>(); // guarded by synchronizing on 'this'

    public IndexDirectoryManager(final RepositoryConfiguration repoConfig) {
        this.repoConfig = repoConfig;
    }

    public synchronized void initialize() {
        final Map<File, Tuple<Long, IndexLocation>> latestIndexByStorageDir = new HashMap<>();

        for (final Map.Entry<String, File> entry : repoConfig.getStorageDirectories().entrySet()) {
            final String partitionName = entry.getKey();
            final File storageDir = entry.getValue();

            final File[] indexDirs = storageDir.listFiles(INDEX_DIRECTORY_FILTER);
            if (indexDirs == null) {
                logger.warn("Unable to access Provenance Repository storage directory {}", storageDir);
                continue;
            }

            for (final File indexDir : indexDirs) {
                final Matcher matcher = INDEX_FILENAME_PATTERN.matcher(indexDir.getName());
                if (!matcher.matches()) {
                    continue;
                }

                final long startTime = DirectoryUtils.getIndexTimestamp(indexDir);
                final List<IndexLocation> dirsForTimestamp = indexLocationByTimestamp.computeIfAbsent(startTime, t -> new ArrayList<>());
                final IndexLocation indexLoc = new IndexLocation(indexDir, partitionName, repoConfig.getDesiredIndexSize());
                dirsForTimestamp.add(indexLoc);

                final Tuple<Long, IndexLocation> tuple = latestIndexByStorageDir.get(storageDir);
                if (tuple == null || startTime > tuple.getKey()) {
                    latestIndexByStorageDir.put(storageDir, new Tuple<>(startTime, indexLoc));
                }
            }
        }

        // Restore the activeIndices to point at the newest index in each storage location.
        for (final Tuple<Long, IndexLocation> tuple : latestIndexByStorageDir.values()) {
            final IndexLocation indexLoc = tuple.getValue();
            activeIndices.put(indexLoc.getPartitionName(), indexLoc);
        }
    }


    public synchronized void deleteDirectory(final File directory) {
        final Iterator<Map.Entry<Long, List<IndexLocation>>> itr = indexLocationByTimestamp.entrySet().iterator();
        while (itr.hasNext()) {
            final Map.Entry<Long, List<IndexLocation>> entry = itr.next();
            final List<IndexLocation> locations = entry.getValue();

            final IndexLocation locToRemove = new IndexLocation(directory, directory.getName(), repoConfig.getDesiredIndexSize());
            locations.remove(locToRemove);
            if (locations.isEmpty()) {
                itr.remove();
            }
        }
    }

    /**
     * Returns a List of all indexes where the latest event in the index has an event time before the given timestamp
     * @param timestamp the cutoff
     * @return all Files that belong to an index, where the index has no events later than the given time
     */
    public synchronized List<File> getDirectoriesBefore(final long timestamp) {
        final List<File> selected = new ArrayList<>();

        // An index cannot be expired if it is the latest index in the storage directory. As a result, we need to
        // separate the indexes by Storage Directory so that we can easily determine if this is the case.
        final Map<String, List<Tuple<Long, IndexLocation>>> startTimeWithFileByStorageDirectory = flattenDirectoriesByTimestamp().stream()
            .collect(Collectors.groupingBy(tuple -> tuple.getValue().getPartitionName()));

        // Scan through the index directories and the associated index event start time.
        // If looking at index N, we can determine the index end time by assuming that it is the same as the
        // start time of index N+1. So we determine the time range of each index and select an index only if
        // its start time is before the given timestamp and its end time is <= the given timestamp.
        for (final List<Tuple<Long, IndexLocation>> startTimeWithFile : startTimeWithFileByStorageDirectory.values()) {
            for (int i = 0; i < startTimeWithFile.size(); i++) {
                final Tuple<Long, IndexLocation> tuple = startTimeWithFile.get(i);
                final Long indexStartTime = tuple.getKey();
                if (indexStartTime > timestamp) {
                    // If the first timestamp in the index is later than the desired timestamp,
                    // then we are done. We can do this because the list is ordered by monotonically
                    // increasing timestamp as the Tuple key.
                    break;
                }

                if (i < startTimeWithFile.size() - 1) {
                    final Tuple<Long, IndexLocation> nextTuple = startTimeWithFile.get(i + 1);
                    final Long indexEndTime = nextTuple.getKey();
                    if (indexEndTime <= timestamp) {
                        selected.add(tuple.getValue().getIndexDirectory());
                    }
                }
            }
        }

        return selected;
    }

    /**
     * Convert directoriesByTimestamp to a List of Tuples, where key = file start time, value = file
     * This allows us to easily get the 'next' value when iterating over the elements.
     * This is useful because we know that the 'next' value will have a timestamp that is when that
     * file started being written to - which is the same as when this index stopped being written to.
     *
     * @return a List of Tuple&lt;Long, File&gt; where the key is the timestamp of the first event in the corresponding File.
     */
    private List<Tuple<Long, IndexLocation>> flattenDirectoriesByTimestamp() {
        final List<Tuple<Long, IndexLocation>> startTimeWithFile = new ArrayList<>();
        for (final Map.Entry<Long, List<IndexLocation>> entry : indexLocationByTimestamp.entrySet()) {
            final Long timestamp = entry.getKey();
            for (final IndexLocation indexLoc : entry.getValue()) {
                startTimeWithFile.add(new Tuple<>(timestamp, indexLoc));
            }
        }

        return startTimeWithFile;
    }

    public synchronized List<File> getDirectories(final Long startTime, final Long endTime) {
        final List<File> selected = new ArrayList<>();

        // An index cannot be expired if it is the latest index in the partition. As a result, we need to
        // separate the indexes by partition so that we can easily determine if this is the case.
        final Map<String, List<Tuple<Long, IndexLocation>>> startTimeWithFileByStorageDirectory = flattenDirectoriesByTimestamp().stream()
            .collect(Collectors.groupingBy(tuple -> tuple.getValue().getPartitionName()));

        // TODO: This is really complicated. Document it. Or, better yet, refactor it so that it's cleaner!
        for (final List<Tuple<Long, IndexLocation>> startTimeWithFile : startTimeWithFileByStorageDirectory.values()) {
            for (int i = 0; i < startTimeWithFile.size(); i++) {
                final Tuple<Long, IndexLocation> tuple = startTimeWithFile.get(i);
                final Long start = tuple.getKey();
                if (endTime != null && start > endTime) {
                    continue;
                }

                if (startTime != null) {
                    final Long end;
                    if (i < startTimeWithFile.size() - 1) {
                        final Tuple<Long, IndexLocation> nextTuple = startTimeWithFile.get(i + 1);
                        end = nextTuple.getKey();
                        if (end < startTime) {
                            continue;
                        }
                    }
                }

                selected.add(tuple.getValue().getIndexDirectory());
            }
        }

        return selected;
    }

    /**
     * Notifies the Index Directory Manager that an Index Writer has been committed for the
     * given index directory. This allows the Directory Manager to know that it needs to check
     * the size of the index directory and not return this directory as a writable directory
     * any more if the size has reached the configured threshold.
     *
     * @param indexDir the directory that was written to
     * @return <code>true</code> if the index directory has reached its max threshold and should no
     *         longer be written to, <code>false</code> if the index directory is not full.
     */
    public boolean onIndexCommitted(final File indexDir) {
        final long indexSize = getSize(indexDir);
        if (indexSize >= repoConfig.getDesiredIndexSize()) {
            synchronized (this) {
                String partitionName = null;
                for (final Map.Entry<String, IndexLocation> entry : activeIndices.entrySet()) {
                    if (indexDir.equals(entry.getValue().getIndexDirectory())) {
                        partitionName = entry.getKey();
                        break;
                    }
                }

                if (partitionName == null) {
                    logger.info("Size of Provenance Index at {} is now {}. However, was unable to find the appropriate Active Index to roll over.", indexDir, indexSize);
                } else {
                    logger.info("Size of Provenance Index at {} is now {}. Will close this index and roll over to a new one.", indexDir, indexSize);
                    activeIndices.remove(partitionName);
                }
            }

            return true;
        }

        return false;
    }

    private long getSize(final File indexDir) {
        if (!indexDir.exists()) {
            return 0L;
        }
        if (!indexDir.isDirectory()) {
            throw new IllegalArgumentException("Must specify a directory but specified " + indexDir);
        }

        // List all files in the Index Directory.
        final File[] files = indexDir.listFiles();
        if (files == null) {
            return 0L;
        }

        long sum = 0L;
        for (final File file : files) {
            sum += file.length();
        }

        return sum;
    }

    public synchronized File getActiveIndexDirectory(final String partitionName) {
        final IndexLocation activeIndexLocation = activeIndices.get(partitionName);
        return activeIndexLocation == null ? null : activeIndexLocation.getIndexDirectory();
    }

    /**
     * Provides the File that is the directory for the index that should be written to. If there is no index yet
     * to be written to, or if the index has reached its max size, a new one will be created. The given {@code earliestTimestamp}
     * should represent the event time of the first event that will go into the index. This is used for file naming purposes so
     * that the appropriate directories can be looked up quickly later.
     *
     * @param earliestTimestamp the event time of the first event that will go into a new index, if a new index is created by this call.
     * @param partitionName the name of the partition to write to
     * @return the directory that should be written to
     */
    public synchronized File getWritableIndexingDirectory(final long earliestTimestamp, final String partitionName) {
        IndexLocation indexLoc = activeIndices.get(partitionName);
        if (indexLoc == null || indexLoc.isIndexFull()) {
            indexLoc = new IndexLocation(createIndex(earliestTimestamp, partitionName), partitionName, repoConfig.getDesiredIndexSize());
            logger.debug("Created new Index Directory {}", indexLoc);

            indexLocationByTimestamp.computeIfAbsent(earliestTimestamp, t -> new ArrayList<>()).add(indexLoc);
            activeIndices.put(partitionName, indexLoc);
        }

        return indexLoc.getIndexDirectory();
    }

    private File createIndex(final long earliestTimestamp, final String partitionName) {
        final File storageDir = repoConfig.getStorageDirectories().entrySet().stream()
            .filter(e -> e.getKey().equals(partitionName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Invalid Partition: " + partitionName));
        final File indexDir = new File(storageDir, "index-" + earliestTimestamp);

        return indexDir;
    }
}
