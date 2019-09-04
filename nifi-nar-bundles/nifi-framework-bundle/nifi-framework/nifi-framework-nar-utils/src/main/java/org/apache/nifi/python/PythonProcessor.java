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
package org.apache.nifi.python;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PythonProcessor extends AbstractProcessor {
    private final FlowFileFunction flowFileFunction;
    private volatile Set<Relationship> relationships = null;


    public PythonProcessor(final FlowFileFunction flowFileFunction) {
        this.flowFileFunction = flowFileFunction;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> current = this.relationships;
        if (current != null) {
            return current;
        }

        try {
            return this.relationships = new HashSet<>(Arrays.asList(flowFileFunction.getRelationships()));
        } catch (final Exception e) {
            return Collections.emptySet();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final PythonFlowFile pythonFlowFile = wrapFlowFile(flowFile, session);
        final String relationshipName = flowFileFunction.accept(pythonFlowFile);
        final Relationship relationship = new Relationship.Builder().name(relationshipName).build();

        session.transfer(flowFile, relationship);
    }

    private PythonFlowFile wrapFlowFile(final FlowFile flowFile, final ProcessSession session) {
        final PythonFlowFile pythonFlowFile = new PythonFlowFile() {
            @Override
            public Map<String, String> getAttributes() {
                return flowFile.getAttributes();
            }

            @Override
            public String getAttribute(final String name) {
                return flowFile.getAttribute(name);
            }

            @Override
            public void putAttribute(final String name, final String value) {
                session.putAttribute(flowFile, name, value);
            }

            @Override
            public void removeAttribute(final String name) {
                session.removeAttribute(flowFile, name);
            }

            @Override
            public long getSize() {
                return flowFile.getSize();
            }

            @Override
            public byte[] readContent() throws IOException {
                final byte[] bytes = new byte[(int) flowFile.getSize()];
                try (final InputStream in = session.read(flowFile)) {
                    fillBuffer(in, bytes, true);
                }

                return bytes;
            }

            @Override
            public void writeContent(final byte[] data) throws IOException {
                try (final OutputStream out = session.write(flowFile)) {
                    out.write(data);
                }
            }
        };

        return pythonFlowFile;
    }

    private static int fillBuffer(final InputStream source, final byte[] destination, final boolean ensureCapacity) throws IOException {
        int bytesRead = 0;
        int len;
        while (bytesRead < destination.length) {
            len = source.read(destination, bytesRead, destination.length - bytesRead);
            if (len < 0) {
                if (ensureCapacity) {
                    throw new EOFException();
                } else {
                    break;
                }
            }

            bytesRead += len;
        }

        return bytesRead;
    }
}
