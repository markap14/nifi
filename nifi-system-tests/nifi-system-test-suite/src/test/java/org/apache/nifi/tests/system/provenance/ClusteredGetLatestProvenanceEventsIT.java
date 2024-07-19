package org.apache.nifi.tests.system.provenance;

import org.apache.nifi.tests.system.NiFiInstanceFactory;

public class ClusteredGetLatestProvenanceEventsIT extends GetLatestProvenanceEventsIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }
}
