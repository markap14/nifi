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
package org.apache.nifi.controller;

import org.apache.nifi.annotation.behavior.AllowsAutoScheduling;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.components.validation.VerifiableComponentFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class StandardProcessorNodeTest {

    @Mock
    private ValidationContextFactory validationContextFactory;

    @Mock
    private ProcessScheduler processScheduler;

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    @Mock
    private ReloadComponent reloadComponent;

    @Mock
    private VerifiableComponentFactory verifiableComponentFactory;

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private ValidationTrigger validationTrigger;

    @Mock
    private TerminationAwareLogger terminationAwareLogger;

    private static final BundleCoordinate BUNDLE_COORDINATE = new BundleCoordinate("org.apache.nifi", "nifi-standard-nar", "2.0.0");

    private StandardProcessorNode createProcessorNode(final Processor processor) {
        final LoggableComponent<Processor> loggableComponent = new LoggableComponent<>(processor, BUNDLE_COORDINATE, terminationAwareLogger);
        return new StandardProcessorNode(loggableComponent, UUID.randomUUID().toString(), validationContextFactory,
                processScheduler, controllerServiceProvider, reloadComponent, verifiableComponentFactory,
                extensionManager, validationTrigger);
    }

    @Test
    void testSetMaxConcurrentTasksStoresPositiveValue() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setMaxConcurrentTasks(5);
        assertEquals(5, node.getMaxConcurrentTasks());
    }

    @Test
    void testSetMaxConcurrentTasksRejectsZero() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        assertThrows(IllegalArgumentException.class, () -> node.setMaxConcurrentTasks(0));
    }

    @Test
    void testSetMaxConcurrentTasksRejectsNegativeValue() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        assertThrows(IllegalArgumentException.class, () -> node.setMaxConcurrentTasks(-1));
    }

    @Test
    void testGetEffectiveMaxConcurrentTasksReturnsOneForTriggerSerially() {
        final StandardProcessorNode node = createProcessorNode(new SerialProcessor());
        node.setMaxConcurrentTasks(8);
        assertEquals(1, node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testGetEffectiveMaxConcurrentTasksReturnsSystemMaxForAutoStrategy() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setSchedulingStrategy(SchedulingStrategy.AUTO);
        node.setSystemMaxConcurrentTasks(16);
        node.setMaxConcurrentTasks(5);
        assertEquals(16, node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testGetEffectiveMaxConcurrentTasksReturnsStoredValueWhenBelowSystemMax() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        node.setSystemMaxConcurrentTasks(20);
        node.setMaxConcurrentTasks(5);
        assertEquals(5, node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testGetEffectiveMaxConcurrentTasksCappedBySystemMax() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        node.setSystemMaxConcurrentTasks(10);
        node.setMaxConcurrentTasks(15);
        assertEquals(10, node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testGetEffectiveMaxConcurrentTasksEqualsSystemMaxWhenEqual() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        node.setSystemMaxConcurrentTasks(8);
        node.setMaxConcurrentTasks(8);
        assertEquals(8, node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testGetMaxConcurrentTasksReturnsRawValueEvenWhenExceedingSystemMax() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setSystemMaxConcurrentTasks(10);
        node.setMaxConcurrentTasks(20);
        assertEquals(20, node.getMaxConcurrentTasks());
    }

    @Test
    void testSystemMaxConcurrentTasksDefaultValue() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        node.setMaxConcurrentTasks(100);
        assertEquals(Math.min(100, NiFiProperties.DEFAULT_PROCESSOR_MAX_CONCURRENT_TASKS), node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testSetRunDurationAcceptsZero() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setRunDuration(0, TimeUnit.MILLISECONDS);
        assertEquals(0, node.getRunDuration(TimeUnit.MILLISECONDS));
    }

    @Test
    void testSetRunDurationAcceptsPositiveValue() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        node.setRunDuration(25, TimeUnit.MILLISECONDS);
        assertEquals(25, node.getRunDuration(TimeUnit.MILLISECONDS));
    }

    @Test
    void testSetRunDurationRejectsNegativeValue() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        assertThrows(IllegalArgumentException.class, () -> node.setRunDuration(-1, TimeUnit.MILLISECONDS));
    }

    @Test
    void testTriggerSeriallyWithAutoStrategyReturnsOne() {
        final StandardProcessorNode node = createProcessorNode(new SerialProcessor());
        node.setSchedulingStrategy(SchedulingStrategy.AUTO);
        assertEquals(1, node.getEffectiveMaxConcurrentTasks());
    }

    @Test
    void testApplySystemDefaultSchedulingStrategyChangesStrategyWhenNoAnnotation() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, node.getSchedulingStrategy());
        node.applySystemDefaultSchedulingStrategy(SchedulingStrategy.AUTO);
        assertEquals(SchedulingStrategy.AUTO, node.getSchedulingStrategy());
    }

    @Test
    void testApplySystemDefaultSchedulingStrategyPreservesAnnotation() {
        final StandardProcessorNode node = createProcessorNode(new CronScheduledProcessor());
        assertEquals(SchedulingStrategy.CRON_DRIVEN, node.getSchedulingStrategy());
        node.applySystemDefaultSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        assertEquals(SchedulingStrategy.CRON_DRIVEN, node.getSchedulingStrategy());
    }

    @Test
    void testApplySystemDefaultSchedulingStrategyAutoOverridesDefault() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, node.getSchedulingStrategy());
        node.applySystemDefaultSchedulingStrategy(SchedulingStrategy.AUTO);
        assertEquals(SchedulingStrategy.AUTO, node.getSchedulingStrategy());
    }

    @Test
    void testIsAutoSchedulingAllowedDefaultsToTrue() {
        final StandardProcessorNode node = createProcessorNode(new SimpleProcessor());
        assertTrue(node.isAutoSchedulingAllowed());
    }

    @Test
    void testIsAutoSchedulingAllowedReturnsFalseForAnnotatedProcessor() {
        final StandardProcessorNode node = createProcessorNode(new NoAutoSchedulingProcessor());
        assertFalse(node.isAutoSchedulingAllowed());
    }

    @Test
    void testDefaultSchedulingStrategyFallsBackToTimerDrivenWhenAutoNotAllowed() {
        final StandardProcessorNode node = createProcessorNode(new NoAutoSchedulingProcessor());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, node.getSchedulingStrategy());
    }

    @Test
    void testApplySystemDefaultAutoFallsBackToTimerDrivenWhenAutoNotAllowed() {
        final StandardProcessorNode node = createProcessorNode(new NoAutoSchedulingProcessor());
        node.applySystemDefaultSchedulingStrategy(SchedulingStrategy.AUTO);
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, node.getSchedulingStrategy());
    }

    @Test
    void testApplySystemDefaultTimerDrivenWorksWhenAutoNotAllowed() {
        final StandardProcessorNode node = createProcessorNode(new NoAutoSchedulingProcessor());
        node.applySystemDefaultSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, node.getSchedulingStrategy());
    }

    static class SimpleProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    @TriggerSerially
    static class SerialProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    @DefaultSchedule(strategy = SchedulingStrategy.CRON_DRIVEN, period = "0 0/5 * * * ?")
    static class CronScheduledProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    @AllowsAutoScheduling(false)
    static class NoAutoSchedulingProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

}
