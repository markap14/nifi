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

package org.apache.nifi.provenance.index;

import java.io.Closeable;
import java.util.Map;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.store.EventStore;

public interface EventIndex extends Closeable {

    void initialize(EventStore eventStore);

    /**
     * Adds the given events to the index so that they can be queried later. If the storage location was
     * rolled over when adding these events to the Event Store, the number of events added to the storage
     * location should be provided, as well. Otherwise, <code>null</code> may be passed as the second argument.
     *
     * @param events the events to index
     */
    void addEvents(Map<ProvenanceEventRecord, StorageSummary> events);

    /**
     * Replaces the entries in the appropriate index with the given events
     *
     * @param events the events to add or replace
     */
    void reindexEvents(Map<ProvenanceEventRecord, StorageSummary> events);

    long getSize();

    QuerySubmission submitQuery(Query query, EventAuthorizer authorizer, String userId);

    ComputeLineageSubmission submitLineageComputation(long eventId, NiFiUser user, EventAuthorizer authorizer);

    ComputeLineageSubmission submitLineageComputation(String flowFileUuid, NiFiUser user, EventAuthorizer authorizer);

    ComputeLineageSubmission submitExpandChildren(long eventId, NiFiUser user, EventAuthorizer authorizer);

    ComputeLineageSubmission submitExpandParents(long eventId, NiFiUser user, EventAuthorizer authorizer);

    ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier, NiFiUser user);

    QuerySubmission retrieveQuerySubmission(String queryIdentifier, NiFiUser user);

    long getMinimumEventIdToReindex();
}
