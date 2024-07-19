package org.apache.nifi.web.api.dto.provenance;

import jakarta.xml.bind.annotation.XmlType;

import java.util.List;

@XmlType(name = "latestProvenanceEvents")
public class LatestProvenanceEventsDTO {
    private String componentId;
    private List<ProvenanceEventDTO> provenanceEvents;

    /**
     * @return the ID of the component whose latest events were fetched
     */
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    /**
     * @return the latest provenance events that were recorded for the associated component
     */
    public List<ProvenanceEventDTO> getProvenanceEvents() {
        return provenanceEvents;
    }

    public void setProvenanceEvents(final List<ProvenanceEventDTO> provenanceEvents) {
        this.provenanceEvents = provenanceEvents;
    }
}
