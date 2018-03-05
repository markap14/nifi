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

package org.apache.nifi.offline;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

public class JaxbOfflineFlowSerDe implements OfflineFlowSerializer, OfflineFlowDeserializer {

    private static final JAXBContext jaxbContext;

    static {
        try {
            jaxbContext = JAXBContext.newInstance(VersionedProcessGroup.class);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to initialize JAX-B Context for Serializing and Deserializing Offline Flows", e);
        }
    }


    @Override
    public VersionedProcessGroup deserialize(final InputStream in) throws IOException {
        final BufferedInputStream bufferedIn = new BufferedInputStream(in);
        try {
            return (VersionedProcessGroup) jaxbContext.createUnmarshaller().unmarshal(bufferedIn);
        } catch (final JAXBException e) {
            throw new IOException("Could not parse Offline Flow", e);
        }
    }

    @Override
    public void serialize(final VersionedProcessGroup flow, final OutputStream destination) throws IOException {
        try {
            final Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.marshal(flow, destination);
        } catch (final Exception e) {
            throw new IOException("Could not serializer Offline Flow", e);
        }
    }

}
