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

package org.apache.nifi.cluster.manager;

import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMSystemDiagnosticsSnapshotDTO;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StatusMergerTest {
    @Test
    void testRepositoryDiagnosticsRemainSpecificToEachNode() {
        final Map<String, String> firstDetails = Map.of("Last Content Cleanup Handoff Milliseconds", "2000");
        final Map<String, String> secondDetails = Map.of("Last Content Cleanup Handoff Milliseconds", "20");
        final JVMDiagnosticsSnapshotDTO firstNode = createSnapshot(firstDetails);
        final JVMDiagnosticsSnapshotDTO secondNode = createSnapshot(secondDetails);
        final JVMDiagnosticsSnapshotDTO aggregate = firstNode.clone();
        StatusMerger.merge(aggregate, secondNode, 1000L);

        assertNull(aggregate.getSystemDiagnosticsDto().getFlowFileRepositoryDiagnosticDetails());
        assertEquals(firstDetails, firstNode.getSystemDiagnosticsDto().getFlowFileRepositoryDiagnosticDetails());
        assertEquals(secondDetails, secondNode.getSystemDiagnosticsDto().getFlowFileRepositoryDiagnosticDetails());
    }

    private JVMDiagnosticsSnapshotDTO createSnapshot(final Map<String, String> details) {
        final JVMSystemDiagnosticsSnapshotDTO system = new JVMSystemDiagnosticsSnapshotDTO();
        system.setFlowFileRepositoryDiagnosticDetails(details);
        system.setPhysicalMemoryBytes(1024L);
        system.setMaxHeapBytes(512L);
        system.setGarbageCollectionDiagnostics(List.of());
        final JVMDiagnosticsSnapshotDTO snapshot = new JVMDiagnosticsSnapshotDTO();
        snapshot.setSystemDiagnosticsDto(system);
        return snapshot;
    }
}
