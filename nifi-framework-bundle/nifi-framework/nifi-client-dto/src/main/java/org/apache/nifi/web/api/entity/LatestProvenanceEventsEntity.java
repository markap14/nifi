package org.apache.nifi.web.api.entity;

import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.web.api.dto.provenance.LatestProvenanceEventsDTO;

@XmlRootElement(name = "latestProvenanceEventsEntity")
public class LatestProvenanceEventsEntity extends Entity {
    private LatestProvenanceEventsDTO latestProvenanceEvents;

    /**
     * @return latest provenance events
     */
    public LatestProvenanceEventsDTO getLatestProvenanceEvents() {
        return latestProvenanceEvents;
    }

    public void setLatestProvenanceEvents(LatestProvenanceEventsDTO latestProvenanceEvents) {
        this.latestProvenanceEvents = latestProvenanceEvents;
    }
}
