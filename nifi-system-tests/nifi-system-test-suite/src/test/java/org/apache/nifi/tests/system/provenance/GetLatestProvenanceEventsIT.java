package org.apache.nifi.tests.system.provenance;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GetLatestProvenanceEventsIT extends NiFiSystemIT {

    @Test
    public void testSingleEvent() throws NiFiClientException, IOException, InterruptedException {
        runTest(false);
    }

    @Test
    public void testMultipleEvents() throws NiFiClientException, IOException, InterruptedException {
        runTest(true);
    }

    private void runTest(final boolean autoTerminateReverse) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity reverse = getClientUtil().createProcessor("ReverseContents");

        if (autoTerminateReverse) {
            getClientUtil().setAutoTerminatedRelationships(reverse, Set.of("success", "failure"));
        } else {
            final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
            getClientUtil().createConnection(reverse, terminate, "success");
            getClientUtil().setAutoTerminatedRelationships(reverse, "failure");
        }

        getClientUtil().createConnection(generate, reverse, "success");
        getClientUtil().updateProcessorProperties(generate, Map.of("Text", "Hello, World!"));

        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(reverse);

        final int expectedEventCount = getNumberOfNodes() * (autoTerminateReverse ? 2 : 1);
        waitFor(() -> {
            final LatestProvenanceEventsEntity entity = getNifiClient().getProvenanceClient().getLatestEvents(reverse.getId());
            final List<ProvenanceEventDTO> events = entity.getLatestProvenanceEvents().getProvenanceEvents();
            return events.size() == expectedEventCount;
        });

        final LatestProvenanceEventsEntity entity = getNifiClient().getProvenanceClient().getLatestEvents(reverse.getId());
        final List<ProvenanceEventDTO> events = entity.getLatestProvenanceEvents().getProvenanceEvents();
        final Map<String, Integer> countsByEventType = new HashMap<>();
        for (final ProvenanceEventDTO event : events) {
            assertEquals(reverse.getId(), event.getComponentId());
            final String eventType = event.getEventType();
            countsByEventType.put(eventType, countsByEventType.getOrDefault(eventType, 0) + 1);

            if (getNumberOfNodes() > 1) {
                assertNotNull(event.getClusterNodeId());
            }
        }

        if (autoTerminateReverse) {
            assertEquals(getNumberOfNodes(), countsByEventType.get("DROP").intValue());
        }
        assertEquals(getNumberOfNodes(), countsByEventType.get("CONTENT_MODIFIED").intValue());
    }
}
