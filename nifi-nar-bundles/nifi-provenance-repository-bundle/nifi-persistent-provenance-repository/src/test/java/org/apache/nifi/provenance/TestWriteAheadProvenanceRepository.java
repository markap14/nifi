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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.events.EventReporter;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestWriteAheadProvenanceRepository {

    @Test
    public void testInsertPerformance() throws IOException, InterruptedException {
        final File dir = new File("target/write-performance-test/" + UUID.randomUUID().toString());
        dir.mkdirs();

        final RepositoryConfiguration repoConfig = new RepositoryConfiguration();
        repoConfig.addStorageDirectory("1", dir);
        repoConfig.setSearchableFields(Arrays.asList(SearchableFields.FlowFileUUID, SearchableFields.Filename, SearchableFields.EventTime, SearchableFields.EventType));
        repoConfig.setCompressOnRollover(false);
        repoConfig.setMaxStorageCapacity(10L * 1024 * 1024 * 1024);
        repoConfig.setDesiredIndexSize(500 * 1024 * 1024);
        repoConfig.setMaxEventFileCapacity(500 * 1024 * 1024);
        repoConfig.setIndexThreadPoolSize(2);

        final WriteAheadProvenanceRepository writeAheadRepo = new WriteAheadProvenanceRepository(repoConfig);
        final Authorizer authorizer = Mockito.mock(Authorizer.class);
        writeAheadRepo.initialize(EventReporter.NO_OP, authorizer, Mockito.mock(ProvenanceAuthorizableFactory.class), Mockito.mock(IdentifierLookup.class));

        final int batchSize = 100;

        final List<ProvenanceEventRecord> eventBatch = new ArrayList<>(batchSize);
        for (int i=0; i < batchSize; i++) {
            final ProvenanceEventRecord event = TestUtil.createEvent();
            eventBatch.add(event);
        }

        final int numEvents = 5_000_000;
        final int numThreads = 10;
        final int eventsPerThread = numEvents / numThreads;
        final int iterationsPerThread = eventsPerThread / batchSize;

        for (int j=0; j < 2; j++) {
            final long start = System.nanoTime();

            final List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < iterationsPerThread; i++) {
                            writeAheadRepo.registerEvents(eventBatch);
                        }
                    }
                });

                thread.setName("Insert Thread-" + t);
                thread.start();

                threads.add(thread);
            }

            for (final Thread t : threads) {
                t.join();
            }

            final long nanos = System.nanoTime() - start;
            final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);

            System.out.println("Inserted " + numEvents + " events in " + millis + " milliseconds using " + numThreads + " threads");
        }
    }
}
