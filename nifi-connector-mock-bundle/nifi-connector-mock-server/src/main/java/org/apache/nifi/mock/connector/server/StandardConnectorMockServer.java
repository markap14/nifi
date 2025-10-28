/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyGroupConfiguration;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.status.history.StatusHistoryDumpFactory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.controller.status.history.VolatileComponentStatusRepository;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.validation.RuleViolationsManager;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StandardConnectorMockServer implements ConnectorMockServer {
    private static final String CONNECTOR_ID = "test-connector";

    private Bundle systemBundle;
    private Set<Bundle> bundles;
    private NiFiProperties nifiProperties;
    private FlowController flowController;
    private ExtensionDiscoveringManager extensionManager;
    private ConnectorNode connectorNode;
    private FlowEngine flowEngine;


    @Override
    public void start() {
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, bundles);
        extensionManager.logClassLoaderMapping();

        final FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
        final Authorizer authorizer = new PermitAllAuthorizer();
        final AuditService auditService = new MockAuditService();
        final PropertyEncryptor propertyEncryptor = new NopPropertyEncryptor();
        final BulletinRepository bulletinRepository = new VolatileBulletinRepository();
        final StatusHistoryRepository statusHistoryRepository = new VolatileComponentStatusRepository(nifiProperties);
        final RuleViolationsManager ruleViolationManager = new MockRuleViolationsManager();
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();

        flowController = FlowController.createStandaloneInstance(
            flowFileEventRepository,
            null,
            nifiProperties,
            authorizer,
            auditService,
            propertyEncryptor,
            bulletinRepository,
            extensionManager,
            statusHistoryRepository,
            ruleViolationManager,
            stateManagerProvider);

        try {
            flowController.getRepositoryContextFactory().getFlowFileRepository().loadFlowFiles(Collections::emptyList);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to initialize FlowFile Repository", e);
        }

        flowEngine = new FlowEngine(4, "Connector Threads");
    }

    @Override
    public void initialize(final NiFiProperties properties, final Bundle systemBundle, final Set<Bundle> bundles, final ExtensionMapping extensionMapping) {
        this.systemBundle = systemBundle;
        this.bundles = bundles;
        this.nifiProperties = properties;
    }

    @Override
    public void stop() {
        if (flowEngine != null) {
            flowEngine.shutdownNow();
        }
        if (flowController != null) {
            flowController.shutdown(true);
        }
    }

    @Override
    public DiagnosticsFactory getDiagnosticsFactory() {
        return null;
    }

    @Override
    public DiagnosticsFactory getThreadDumpFactory() {
        return null;
    }

    @Override
    public DecommissionTask getDecommissionTask() {
        return null;
    }

    @Override
    public ClusterDetailsFactory getClusterDetailsFactory() {
        return null;
    }

    @Override
    public StatusHistoryDumpFactory getStatusHistoryDumpFactory() {
        return null;
    }

    @Override
    public void instantiateConnector(final String connectorClassName) {
        final List<Bundle> bundles = extensionManager.getBundles(connectorClassName);
        if (bundles.isEmpty()) {
            throw new IllegalStateException("No bundles found for connector class: " + connectorClassName + " - ensure that you have included all relevant NARs in the configured lib directory");
        }
        if (bundles.size() > 1) {
            throw new IllegalStateException("Multiple bundles found for connector class: " + connectorClassName + " - unable to determine which bundle to use. Ensure that only a single version of " +
                                            "the Connector is included in the configured lib directory. Available bundles: " + bundles);
        }

        final BundleCoordinate bundleCoordinate = bundles.getFirst().getBundleDetails().getCoordinate();
        connectorNode = flowController.getFlowManager().createConnector(connectorClassName, CONNECTOR_ID, bundleCoordinate, true, true);
    }

    @Override
    public void prepareForUpdate() throws FlowUpdateException {
        connectorNode.prepareForUpdate(flowEngine);
    }

    @Override
    public void finishUpdate() throws FlowUpdateException {
        connectorNode.finishUpdate(flowEngine);
    }

    @Override
    public void configure(final String stepName, final List<PropertyGroupConfiguration> groupConfigurations) throws FlowUpdateException {
        connectorNode.setConfiguration(stepName, groupConfigurations);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfiguration(final String stepName, final List<PropertyGroupConfiguration> groupConfigurations) {
        return connectorNode.verifyConfigurationStep(stepName, groupConfigurations);
    }

    @Override
    public void startConnector() {
        connectorNode.start(flowEngine);
    }

    @Override
    public void stopConnector() {
        connectorNode.stop(flowEngine);
    }

    @Override
    public void waitForDataIngested(final Duration maxWaitTime) {
        final long startTime = System.currentTimeMillis();
        final long expirationTime = startTime + maxWaitTime.toMillis();

        while (connectorNode.getFlowFileTransferCounts().getReceivedCount() == 0L) {
            if (System.currentTimeMillis() > expirationTime) {
                throw new RuntimeException("Timed out waiting for data to be ingested by the Connector");
            }

            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for data to be ingested by the Connector", e);
            }
        }
    }

    @Override
    public void waitForIdle(final Duration maxWaitTime) {
        waitForIdle(Duration.ofMillis(0L), maxWaitTime);
    }

    @Override
    public void waitForIdle(final Duration minimumIdleTime, final Duration maxWaitTime) {
        Optional<Duration> idleTime = connectorNode.getIdleDuration();

        // Wait until idleTime is not empty and is at least equal to minimumIdleTime
        final long startTime = System.currentTimeMillis();
        final long expirationTime = startTime + maxWaitTime.toMillis();

        while (idleTime.isEmpty() || idleTime.get().compareTo(minimumIdleTime) <= 0) {
            if (System.currentTimeMillis() > expirationTime) {
                throw new RuntimeException("Timed out waiting for Connector to be idle");
            }

            try {
                Thread.sleep(100L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Connector to be idle", e);
            }
            idleTime = connectorNode.getIdleDuration();
        }
    }

    @Override
    public List<ValidationResult> validate() {
        final ValidationState validationState = connectorNode.performValidation();
        return validationState.getValidationErrors().stream()
            .filter(result -> !result.isValid())
            .filter(result -> !DisabledServiceValidationResult.isMatch(result))
            .toList();
    }

    @Override
    public void close() {
        stop();
    }
}
