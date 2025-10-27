/*
 *  Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MockRuleViolationsManager implements RuleViolationsManager {
    @Override
    public void upsertComponentViolations(final String subjectId, final Collection<RuleViolation> violations) {

    }

    @Override
    public void upsertGroupViolations(final VersionedProcessGroup processGroup, final Collection<RuleViolation> violations, final Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations) {

    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForSubject(final String subjectId) {
        return List.of();
    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForGroup(final String groupId) {
        return List.of();
    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForGroups(final Collection<String> groupIds) {
        return List.of();
    }

    @Override
    public Collection<RuleViolation> getAllRuleViolations() {
        return List.of();
    }

    @Override
    public void removeRuleViolationsForSubject(final String subjectId) {

    }

    @Override
    public void removeRuleViolationsForRule(final String ruleId) {

    }

    @Override
    public void cleanUp() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
