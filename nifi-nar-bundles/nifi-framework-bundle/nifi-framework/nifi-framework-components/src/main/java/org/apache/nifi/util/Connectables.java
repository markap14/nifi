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
package org.apache.nifi.util;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.status.FlowFileAvailability;
import org.apache.nifi.processor.Relationship;

import java.util.Collection;
import java.util.List;

public class Connectables {

    public static boolean flowFilesQueued(final Connectable connectable) {
        // This method is called A LOT. Because of this, the creation of an Iterator becomes expensive to do over & over. As a result,
        // we use the old-style index to iterate over the List, which is more efficient, especially on garbage collection.
        final List<Connection> connections = connectable.getIncomingConnections();
        for (int i=0; i < connections.size(); i++) {
            final Connection conn = connections.get(i);

            if (conn.getFlowFileQueue().getFlowFileAvailability() == FlowFileAvailability.FLOWFILE_AVAILABLE) {
                return true;
            }
        }

        return false;
    }

    public static boolean anyRelationshipAvailable(final Connectable connectable) {
        for (final Relationship relationship : connectable.getRelationships()) {
            final Collection<Connection> connections = connectable.getConnections(relationship);

            boolean available = true;
            for (final Connection connection : connections) {
                if (connection.getFlowFileQueue().isFull()) {
                    available = false;
                    break;
                }
            }

            if (available) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasNonLoopConnection(final Connectable connectable) {
        final List<Connection> connections = connectable.getIncomingConnections();
        for (final Connection connection : connections) {
            if (!connection.getSource().equals(connectable)) {
                return true;
            }
        }

        return false;
    }
}
