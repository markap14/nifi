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

package org.apache.nifi.cluster.asset;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetRequestReplicator;
import org.apache.nifi.asset.StandardAsset;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AssetDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class OkHttpAssetRequestReplicator implements AssetRequestReplicator {
    private static final Logger logger = LoggerFactory.getLogger(OkHttpAssetRequestReplicator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ClusterCoordinator clusterCoordinator;
    private final NiFiProperties properties;
    private final OkHttpClient okHttpClient;


    public OkHttpAssetRequestReplicator(final ClusterCoordinator clusterCoordinator, final NiFiProperties properties) throws TlsException {
        this.clusterCoordinator = clusterCoordinator;
        this.properties = properties;
        this.okHttpClient = createClient();
    }

    private OkHttpClient createClient() throws TlsException {
        if (clusterCoordinator == null) {
            throw new IllegalStateException("Cluster Coordinator has not been set");
        }
        if (properties == null) {
            throw new IllegalStateException("NiFi Properties have not been set");
        }

        final long readTimeoutMillis = FormatUtils.getTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
        final Duration timeout = Duration.ofMillis(readTimeoutMillis);
        final OkHttpClient okHttpClient;
        if (properties.isTlsConfigurationPresent()) {
            final TlsConfiguration tlsConfig = StandardTlsConfiguration.fromNiFiProperties(properties);
            final X509TrustManager trustManager = SslContextFactory.getX509TrustManager(tlsConfig);
            final SSLContext sslContext = SslContextFactory.createSslContext(tlsConfig, new TrustManager[] {trustManager});

            okHttpClient = createOkHttpClient(sslContext, trustManager, timeout);
        } else {
            okHttpClient = createOkHttpClient(null, null, timeout);
        }

        logger.info("Successfully initialized OkHttpAssetRequestReplicator");
        return okHttpClient;
    }

    private OkHttpClient createOkHttpClient(final SSLContext sslContext, final X509TrustManager trustManager, final Duration timeout) {
        final OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient().newBuilder();
        okHttpClientBuilder.connectTimeout(timeout);
        okHttpClientBuilder.readTimeout(timeout);
        okHttpClientBuilder.followRedirects(true);

        if (sslContext != null && trustManager != null) {
            okHttpClientBuilder.followSslRedirects(true);
            okHttpClientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }

        return okHttpClientBuilder.build();
    }

    @Override
    public Asset createAsset(final String assetName, final InputStream contents, final URI exampleRequestUri) throws IOException {
        final File tempFile = Files.createTempFile("nifi", "asset").toFile();
        logger.debug("Created temporary file {} to hold contents of asset {}", tempFile.getAbsolutePath(), assetName);

        try {
            Files.copy(contents, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            final Set<NodeIdentifier> nodeIds = clusterCoordinator.getNodeIdentifiers();
            final Map<NodeIdentifier, Future<Asset>> futures = new HashMap<>();
            for (final NodeIdentifier nodeId : nodeIds) {
                final Future<Asset> future = createAssetAsync(nodeId, exampleRequestUri, assetName, tempFile);
                futures.put(nodeId, future);
            }

            Asset returnAsset = null;
            for (final Map.Entry<NodeIdentifier, Future<Asset>> entry : futures.entrySet()) {
                final NodeIdentifier nodeId = entry.getKey();
                final Future<Asset> future = entry.getValue();
                try {
                    returnAsset = future.get();
                    logger.debug("Node {} successfully created asset {}", nodeId, assetName);
                } catch (final ExecutionException ee) {
                    throw new IOException("Failed to replicate request to create asset to " + nodeId, ee.getCause());
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for request to create asset to replicate to " + nodeId, e);
                } catch (final Exception e) {
                    throw new IOException("Failed to replicate request to create asset to " + nodeId, e);
                }
            }

            return returnAsset;
        } finally {
            final boolean successfulDelete = tempFile.delete();
            if (successfulDelete) {
                logger.debug("Deleted temporary file {} that was created to hold contents of asset {}", tempFile.getAbsolutePath(), assetName);
            } else {
                logger.warn("Failed to delete temporary file {}. This file should be cleaned up manually", tempFile.getAbsolutePath());
            }
        }
    }

    private Future<Asset> createAssetAsync(final NodeIdentifier nodeId, final URI exampleRequestUri, final String assetName, final File contents) {
        final CompletableFuture<Asset> future = new CompletableFuture<>();
        Thread.ofVirtual().name("Replicate Asset to " + nodeId.getApiAddress()).start(() -> {
            try {
                final Asset asset = replicateRequest(nodeId, exampleRequestUri, assetName, contents);
                logger.debug("Successfully replicated request to create asset {} to {}", assetName, nodeId.getApiAddress());

                future.complete(asset);
            } catch (final IOException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private Asset replicateRequest(final NodeIdentifier nodeId, final URI exampleRequestUri, final String assetName, final File contents) throws IOException {
        final MultipartBody requestBody = new MultipartBody.Builder()
            .setType(MultipartBody.FORM)
            .addFormDataPart("assetName", assetName)
            .addFormDataPart("file", assetName, RequestBody.create(contents, MediaType.parse("application/octet-stream")))
            .build();

        final String schema = exampleRequestUri.getScheme();
        final String address = nodeId.getApiAddress();
        final int port = nodeId.getApiPort();
        final String path = exampleRequestUri.getPath();
        final URI uri;
        try {
            uri = new URI(schema, null, address, port, path, null, null);
        } catch (final URISyntaxException e) {
            throw new IOException(e);
        }

        final Request request = new Request.Builder()
            .url(uri.toURL())
            .post(requestBody)
            .addHeader("content-type", "multipart/form-data")

            // Special NiFi-specific headers to indicate that the request should be performed and not replicated to the nodes
            .addHeader("X-Execution-Continue", "true")
            .addHeader("X-Request-Replicated", "true")
            .build();

        final Call call = okHttpClient.newCall(request);

        logger.debug("Replicating Asset creation request for {} to {}", assetName, nodeId);
        try (final Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Failed to replicate request to create asset to " + nodeId + " due to " + response.code() + " " + response.message());
            }

            final ResponseBody responseBody = response.body();
            if (responseBody == null) {
                throw new IOException("Failed to replicate request to create asset to " + nodeId + ": received a successful response but the response body was empty");
            }

            final AssetEntity assetEntity = objectMapper.readValue(responseBody.byteStream(), AssetEntity.class);
            final AssetDTO assetDto = assetEntity.getAsset();
            return new StandardAsset(assetDto.getId(), assetDto.getName(), new File(assetDto.getFilename()));
        }
    }
}
