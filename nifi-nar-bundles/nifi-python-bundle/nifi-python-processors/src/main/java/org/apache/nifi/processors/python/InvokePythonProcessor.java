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
package org.apache.nifi.processors.python;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.python.FlowFileOperator;
import org.apache.nifi.python.PythonFlowFile;
import org.apache.nifi.stream.io.StreamUtils;
import py4j.ClientServer;
import py4j.GatewayServer;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

public class InvokePythonProcessor extends AbstractProcessor {
    private static final Class[] entryPointTypes = new Class[]{FlowFileOperator.class};

    static final PropertyDescriptor PYTHON_PORT = new Builder()
        .name("Py4J Port")
        .displayName("Py4J Port")
        .description("The Port that the Python Process's Gateway is listening on")
        .required(true)
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .defaultValue("25333")
        .build();

    private volatile int pythonPort = -1;
    private volatile Set<Relationship> relationships = null;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PYTHON_PORT);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> current = this.relationships;
        if (current != null) {
            return current;
        }

        final int port = pythonPort;
        if (port == -1) {
            return Collections.emptySet();
        }

        try {
            return this.relationships = withFlowFileOperator(operator -> operator.getRelationships().stream()
                .map(relationship -> new Relationship.Builder().name(relationship.getName()).build())
                .collect(Collectors.toSet()));
        } catch (final Exception e) {
            return Collections.emptySet();
        }
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(PYTHON_PORT)) {
            pythonPort = Integer.parseInt(newValue);
        }

        relationships = null;
    }

    private volatile ClientServer clientServer;
    private volatile FlowFileOperator operator;

    private <T> T withFlowFileOperator(final Function<FlowFileOperator, T> function) {
        final FlowFileOperator flowFileOperator;
        synchronized (this) {
            if (clientServer == null) {
                clientServer = new ClientServer(0, GatewayServer.defaultAddress(), pythonPort, GatewayServer.defaultAddress(), 5000, 60_000, ServerSocketFactory.getDefault(),
                    SocketFactory.getDefault(), null);
                operator = (FlowFileOperator) clientServer.getPythonServerEntryPoint(entryPointTypes);
            }

            flowFileOperator = operator;
        }

        try {
            return function.apply(flowFileOperator);
        } catch (final Exception e) {
            synchronized (this) {
                clientServer.shutdown();

                clientServer = null;
                operator = null;
            }

            throw e;
        }
    }

    @OnStopped
    public synchronized void shutdown() {
        if (clientServer != null) {
            clientServer.shutdown();
            clientServer = null;
            operator = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final PythonFlowFile pythonFlowFile = wrapFlowFile(flowFile, session);
        final String relationshipName = withFlowFileOperator(operator ->
            operator.accept(pythonFlowFile)
        );

        session.transfer(flowFile, new Relationship.Builder().name(relationshipName).build());
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
                    StreamUtils.fillBuffer(in, bytes);
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
}
