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

import java.io.File;
import java.io.IOException;

import javax.net.ssl.SSLContext;

import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;

public class FileSystemWorkspaceRepositoryFactoryBean implements FactoryBean<WorkspaceRepository> {
    private NiFiProperties properties;
    private FlowRegistryClient flowRegistryClient;

    @Override
    public WorkspaceRepository getObject() throws IOException {
        final SSLContext sslContext = SslContextFactory.createSslContext(this.properties, false);
        final JaxbOfflineFlowSerDe offlineFlowSerde = new JaxbOfflineFlowSerDe();
        final String workspaceStorageDirName = properties.getProperty(NiFiProperties.WORKSPACE_STORAGE_DIRECTORY, NiFiProperties.DEFAULT_WORKSPACE_STORAGE_DIRECTORY);
        final File workspaceStorageDir = new File(workspaceStorageDirName);

        final PropertiesFileWorkspaceSerDe workspaceSerde = new PropertiesFileWorkspaceSerDe(offlineFlowSerde, flowRegistryClient, sslContext);
        final WorkspaceRepository workspaceRepo = new FileSystemWorkspaceRepository(workspaceSerde, workspaceSerde, workspaceStorageDir, offlineFlowSerde, offlineFlowSerde);
        return workspaceRepo;
    }

    @Override
    public Class<?> getObjectType() {
        return WorkspaceRepository.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    public void setFlowRegistryClient(final FlowRegistryClient flowRegistryClient) {
        this.flowRegistryClient = flowRegistryClient;
    }
}
