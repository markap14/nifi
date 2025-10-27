/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MockAuditService implements AuditService {
    @Override
    public void addActions(final Collection<Action> actions) {
    }

    @Override
    public Map<String, List<PreviousValue>> getPreviousValues(final String componentId) {
        return Map.of();
    }

    @Override
    public void deletePreviousValues(final String propertyName, final String componentId) {
    }

    @Override
    public History getActions(final HistoryQuery actionQuery) {
        return null;
    }

    @Override
    public History getActions(final int firstActionId, final int maxActions) {
        return null;
    }

    @Override
    public Action getAction(final Integer actionId) {
        return null;
    }

    @Override
    public void purgeActions(final Date end, final Action purgeAction) {
    }
}
