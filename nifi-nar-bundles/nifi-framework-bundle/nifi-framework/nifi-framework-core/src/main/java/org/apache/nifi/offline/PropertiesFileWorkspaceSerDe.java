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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.UUID;

import javax.net.ssl.SSLContext;

import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.RestBasedFlowRegistry;

public class PropertiesFileWorkspaceSerDe implements WorkspaceSerializer, WorkspaceDeserializer {
    private static final String FLOW_DIRECTORY_NAME = "flow";

    private static final String PROPERTIES_FILENAME = "workspace.properties";
    private static final String IDENTIFIER = "identifier";
    private static final String OWNER = "owner";
    private static final String LABEL = "label";

    private static final String CURRENT_REVISION = "current.revision";
    private static final String MIN_REVISION = "min.revision";
    private static final String MAX_REVISION = "max.revision";

    private static final String FLOW_REGISTRY_URL = "flow.registry.url";
    private static final String C2_IDENTIFIER = "c2.identifier";
    private static final String C2_URL = "c2.url";

    private final OfflineFlowDeserializer flowDeserializer;
    private final FlowRegistryClient flowRegistryClient;
    private final SSLContext sslContext;

    public PropertiesFileWorkspaceSerDe(final OfflineFlowDeserializer flowDeserializer, final FlowRegistryClient flowRegistryClient, final SSLContext sslContext) {
        this.flowDeserializer = flowDeserializer;
        this.flowRegistryClient = flowRegistryClient;
        this.sslContext = sslContext;
    }

    @Override
    public Workspace getWorkspace(final String location) throws IOException {
        final File workspaceDir = new File(location);
        final File propertiesFile = new File(workspaceDir, PROPERTIES_FILENAME);

        final Properties properties = new Properties();
        try (final InputStream fis = new FileInputStream(propertiesFile);
            final InputStream in = new BufferedInputStream(fis)) {
            properties.load(in);
        }

        return fromProperties(properties, workspaceDir);
    }

    @Override
    public void serialize(final Workspace workspace, final String location) throws IOException {
        final File workspaceDir = new File(location);
        final File propertiesFile = new File(workspaceDir, PROPERTIES_FILENAME);

        final Properties properties = toProperties(workspace);

        try (final OutputStream fos = new FileOutputStream(propertiesFile);
            final OutputStream out = new BufferedOutputStream(fos)) {
            properties.store(out, "");
        }
    }

    private Workspace fromProperties(final Properties properties, final File workspaceDir) {
        final String identifier = properties.getProperty(IDENTIFIER);
        final String owner = properties.getProperty(OWNER);
        final String label = properties.getProperty(LABEL);

        final File flowDirectory = new File(workspaceDir, FLOW_DIRECTORY_NAME);
        final int currentRevision = Integer.parseInt(properties.getProperty(CURRENT_REVISION));
        final int minRevision = Integer.parseInt(properties.getProperty(MIN_REVISION));
        final int maxRevision = Integer.parseInt(properties.getProperty(MAX_REVISION));
        final String flowRegistryUrl = properties.getProperty(FLOW_REGISTRY_URL);
        final String c2Identifier = properties.getProperty(C2_IDENTIFIER);
        final String c2Url = properties.getProperty(C2_URL);

        final OfflineFlow offlineFlow = new FileSystemOfflineFlow(flowDirectory, flowDeserializer, currentRevision, minRevision, maxRevision);

        final FlowRegistry flowRegistry = new RestBasedFlowRegistry(flowRegistryClient, UUID.randomUUID().toString(), flowRegistryUrl, sslContext, "Workspace Client");
        final C2Server c2Server = new HttpC2Server(c2Identifier, c2Url);

        return new StandardWorkspace(identifier, owner, label, flowRegistry, c2Server, offlineFlow);
    }

    private Properties toProperties(final Workspace workspace) {
        final Properties properties = new Properties();

        properties.setProperty(IDENTIFIER, workspace.getIdentifier());
        properties.setProperty(OWNER, workspace.getOwner());
        properties.setProperty(LABEL, workspace.getLabel());

        final OfflineFlow flow = workspace.getFlow();
        properties.setProperty(CURRENT_REVISION, String.valueOf(flow.getCurrentRevision()));
        properties.setProperty(MIN_REVISION, String.valueOf(flow.getMinAvailableRevision()));
        properties.setProperty(MAX_REVISION, String.valueOf(flow.getMaxRevision()));

        final FlowRegistry flowRegistry = workspace.getFlowRegistry();
        final String flowRegistryUrl = flowRegistry.getURL();
        properties.setProperty(FLOW_REGISTRY_URL, flowRegistryUrl);

        final C2Server c2Server = workspace.getC2Server();
        properties.setProperty(C2_IDENTIFIER, c2Server.getIdentifier());
        properties.setProperty(C2_URL, c2Server.getURL());

        return properties;
    }

}
