/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.logging;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.Severity;

public class ConnectorLogObserver implements LogObserver {
    private static final String CATEGORY = "Log Message";

    private final BulletinRepository bulletinRepository;
    private final ConnectorNode connectorNode;

    public ConnectorLogObserver(final BulletinRepository bulletinRepository, final ConnectorNode connectorNode) {
        this.bulletinRepository = bulletinRepository;
        this.connectorNode = connectorNode;
    }

    @Override
    public void onLogMessage(final LogMessage message) {
        // Map LogLevel.WARN to Severity.WARNING so that we are consistent with the Severity enumeration. Else, just use whatever
        // the LogLevel is (INFO and ERROR map directly and all others we will just accept as they are).
        final String bulletinLevel = (message.getLogLevel() == LogLevel.WARN) ? Severity.WARNING.name() : message.getLogLevel().toString();
        bulletinRepository.addBulletin(BulletinFactory.createBulletin(connectorNode, CATEGORY, bulletinLevel, message.getMessage()));
    }

    @Override
    public String getComponentDescription() {
        return connectorNode.toString();
    }

}
