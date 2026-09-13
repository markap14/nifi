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
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.GarbageCollectionLog;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class VirtualThreadSchedulingAgentTest {
    private static final int MAX_THREADS = 10;
    private static final String COMPONENT_ID = UUID.randomUUID().toString();

    @Mock
    private FlowController flowController;

    @Mock
    private FlowEngine flowEngine;

    @Mock
    private RepositoryContextFactory contextFactory;

    @Mock
    private NiFiProperties nifiProperties;

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private StateManager stateManager;

    @Mock
    private GarbageCollectionLog garbageCollectionLog;

    @Mock
    private ExtensionManager extensionManager;

    private VirtualThreadSchedulingAgent agent;

    @BeforeEach
    void setUp() {
        when(nifiProperties.getBoredYieldDuration()).thenReturn("10 millis");
        when(nifiProperties.getMaxConcurrentTasks()).thenReturn(12);
        final VirtualThreadScaler noOpScaler = (connectable, scalingState) -> ScalingRecommendation.HOLD;
        agent = new VirtualThreadSchedulingAgent(flowController, flowEngine, contextFactory, nifiProperties, MAX_THREADS, noOpScaler);
    }

    @AfterEach
    void tearDown() {
        agent.shutdown();
    }

    @Test
    void testSetMaxThreadCountAdjustsSemaphore() {
        assertEquals(MAX_THREADS, agent.getGlobalSemaphore().getMaxPermits());
        agent.setMaxThreadCount(20);
        assertEquals(20, agent.getGlobalSemaphore().getMaxPermits());
        agent.setMaxThreadCount(5);
        assertEquals(5, agent.getGlobalSemaphore().getMaxPermits());
    }

    @Test
    void testIncrementMaxThreadCountIsNoOp() {
        final int originalPermits = agent.getGlobalSemaphore().getMaxPermits();
        agent.incrementMaxThreadCount(5);
        assertEquals(originalPermits, agent.getGlobalSemaphore().getMaxPermits());
    }

    @Test
    void testAdministrativeYieldDuration() {
        agent.setAdministrativeYieldDuration("5 sec");
        assertEquals("5 sec", agent.getAdministrativeYieldDuration());
        assertEquals(5000L, agent.getAdministrativeYieldDuration(TimeUnit.MILLISECONDS));
    }

    @Test
    void testScheduleSpawnsThreadsThatInvoke() throws InterruptedException {
        final int concurrentTasks = 3;
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final CountDownLatch allTasksInvoked = new CountDownLatch(concurrentTasks);

        final Connectable connectable = createFullyMockedConnectable(concurrentTasks, invocationCount, allTasksInvoked);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        agent.schedule(connectable, lifecycleState);

        assertTrue(allTasksInvoked.await(5, TimeUnit.SECONDS),
                "Expected " + concurrentTasks + " threads to invoke, but only " + (concurrentTasks - allTasksInvoked.getCount()) + " did");
        assertTrue(invocationCount.get() >= concurrentTasks,
                "Expected at least " + concurrentTasks + " invocations but got " + invocationCount.get());

        lifecycleState.setScheduled(false);
        Thread.sleep(100);
    }

    @Test
    void testUnscheduleExitsQuickly() throws InterruptedException {
        final Connectable connectable = createFullyMockedConnectable(2, new AtomicInteger(), new CountDownLatch(0));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        agent.schedule(connectable, lifecycleState);
        Thread.sleep(300);

        agent.unschedule(connectable, lifecycleState);
        Thread.sleep(100);

        assertEquals(0, lifecycleState.getActiveThreadCount(),
                "Active threads should be 0 after unschedule + 100ms");
    }

    @Test
    void testSemaphoreLimitsConcurrentInvocations() throws InterruptedException {
        final int semaphorePermits = 2;
        final int totalThreads = 5;
        agent.setMaxThreadCount(semaphorePermits);

        final AtomicInteger concurrentCount = new AtomicInteger(0);
        final AtomicInteger maxObservedConcurrency = new AtomicInteger(0);
        final CountDownLatch allDone = new CountDownLatch(totalThreads);

        for (int i = 0; i < totalThreads; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    agent.getGlobalSemaphore().acquire();
                    try {
                        final int current = concurrentCount.incrementAndGet();
                        maxObservedConcurrency.accumulateAndGet(current, Math::max);
                        Thread.sleep(50);
                    } finally {
                        concurrentCount.decrementAndGet();
                        agent.getGlobalSemaphore().release();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    allDone.countDown();
                }
            });
        }

        assertTrue(allDone.await(5, TimeUnit.SECONDS));
        assertTrue(maxObservedConcurrency.get() <= semaphorePermits,
                "Max concurrency " + maxObservedConcurrency.get() + " exceeded semaphore permits " + semaphorePermits);
    }

    @Test
    void testScheduleOnceInvokesAndStops() throws InterruptedException {
        final Connectable connectable = createFullyMockedConnectable(1, new AtomicInteger(), new CountDownLatch(0));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        final CountDownLatch stopCallbackInvoked = new CountDownLatch(1);

        agent.scheduleOnce(connectable, lifecycleState, () -> {
            stopCallbackInvoked.countDown();
            return null;
        });

        assertTrue(stopCallbackInvoked.await(5, TimeUnit.SECONDS),
                "Stop callback should have been invoked after scheduleOnce");
    }

    @Test
    void testAutoStrategyStartsAtOne() throws InterruptedException {
        final Connectable connectable = createFullyMockedConnectable(1, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getSchedulingStrategy()).thenReturn(SchedulingStrategy.AUTO);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        agent.schedule(connectable, lifecycleState);
        Thread.sleep(100);

        assertEquals(1, agent.getTargetConcurrentTasks(connectable));

        lifecycleState.setScheduled(false);
        Thread.sleep(100);
    }

    @Test
    void testTriggerSeriallyProcessorInAutoModeCapsAtOneConcurrentTask() throws InterruptedException {
        final ProcessorNode processorNode = createMockedProcessorNode(SchedulingStrategy.AUTO, 1, 12);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        // SCALE_UP requests must be capped because the effective max is 1 regardless of systemMax.
        final VirtualThreadScaler scaleUpScaler = (connectable, scalingState) -> ScalingRecommendation.SCALE_UP;
        final VirtualThreadSchedulingAgent triggerSeriallyAgent = new VirtualThreadSchedulingAgent(flowController, flowEngine,
                contextFactory, nifiProperties, MAX_THREADS, scaleUpScaler);

        try {
            triggerSeriallyAgent.schedule(processorNode, lifecycleState);
            Thread.sleep(100);
            assertEquals(1, triggerSeriallyAgent.getTargetConcurrentTasks(processorNode));
        } finally {
            lifecycleState.setScheduled(false);
            Thread.sleep(100);
            triggerSeriallyAgent.shutdown();
        }
    }

    @Test
    void testActiveThreadCountReflectsSemaphoreUsage() throws InterruptedException {
        agent.setMaxThreadCount(4);
        assertEquals(0, agent.getActiveThreadCount());

        final CountDownLatch acquired = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < 2; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    agent.getGlobalSemaphore().acquire();
                    acquired.countDown();
                    release.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    agent.getGlobalSemaphore().release();
                }
            });
        }

        assertTrue(acquired.await(5, TimeUnit.SECONDS));
        assertEquals(2, agent.getActiveThreadCount());
        release.countDown();
    }

    @Test
    void testUnscheduleRemovesScalingState() throws InterruptedException {
        final Connectable connectable = createFullyMockedConnectable(1, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getSchedulingStrategy()).thenReturn(SchedulingStrategy.AUTO);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        agent.schedule(connectable, lifecycleState);
        Thread.sleep(100);
        assertEquals(1, agent.getTargetConcurrentTasks(connectable));

        agent.unschedule(connectable, lifecycleState);
        Thread.sleep(100);
        assertEquals(-1, agent.getTargetConcurrentTasks(connectable));
    }

    @Test
    void testAutoStrategySleepsWhenYielded() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final Connectable connectable = createFullyMockedConnectable(1, invocationCount, new CountDownLatch(0));
        when(connectable.getSchedulingStrategy()).thenReturn(SchedulingStrategy.AUTO);
        when(connectable.getYieldExpiration()).thenReturn(System.currentTimeMillis() + 5000L);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        agent.schedule(connectable, lifecycleState);

        Thread.sleep(500);
        final int countAfterWait = invocationCount.get();
        assertTrue(countAfterWait < 20,
                "Expected fewer than 20 invocations during yield (not tight-spinning), but got " + countAfterWait);

        lifecycleState.setScheduled(false);
        Thread.sleep(100);
    }

    @Test
    void testAutoStrategyExitsOnTerminated() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final Connectable connectable = createFullyMockedConnectable(1, invocationCount, new CountDownLatch(0));
        when(connectable.getSchedulingStrategy()).thenReturn(SchedulingStrategy.AUTO);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        agent.schedule(connectable, lifecycleState);
        Thread.sleep(100);
        assertTrue(invocationCount.get() > 0);

        lifecycleState.terminate();
        Thread.sleep(200);

        final int countAfterTerminate = invocationCount.get();
        Thread.sleep(200);
        assertEquals(countAfterTerminate, invocationCount.get(),
                "No additional invocations should occur after termination");

        lifecycleState.setScheduled(false);
    }

    private ProcessorNode createMockedProcessorNode(final SchedulingStrategy schedulingStrategy,
                                                     final int effectiveMaxConcurrentTasks,
                                                     final int systemMaxConcurrentTasks) {
        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(processorNode.getName()).thenReturn("SerialProcessor");
        when(processorNode.getSchedulingStrategy()).thenReturn(schedulingStrategy);
        when(processorNode.getEffectiveMaxConcurrentTasks()).thenReturn(effectiveMaxConcurrentTasks);
        when(processorNode.getMaxConcurrentTasks()).thenReturn(systemMaxConcurrentTasks);
        when(processorNode.getIncomingConnections()).thenReturn(Collections.emptyList());
        when(processorNode.getRelationships()).thenReturn(Collections.emptySet());
        when(processorNode.getSchedulingPeriod(TimeUnit.MILLISECONDS)).thenReturn(100L);
        when(processorNode.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(100L));
        when(processorNode.getYieldExpiration()).thenReturn(0L);
        when(processorNode.isTriggerWhenEmpty()).thenReturn(true);
        when(processorNode.isIsolated()).thenReturn(false);
        when(processorNode.getRunDuration(TimeUnit.NANOSECONDS)).thenReturn(0L);
        when(processorNode.isSessionBatchingSupported()).thenReturn(false);
        when(processorNode.getScheduledState()).thenReturn(ScheduledState.RUNNING);
        final Processor runnableComponent = mock(Processor.class);
        when(processorNode.getRunnableComponent()).thenReturn(runnableComponent);

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getName()).thenReturn("RootGroup");
        when(processGroup.getParent()).thenReturn(null);
        when(processorNode.getProcessGroup()).thenReturn(processGroup);

        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(stateManagerProvider.getStateManager(eq(COMPONENT_ID))).thenReturn(stateManager);
        when(flowController.getGarbageCollectionLog()).thenReturn(garbageCollectionLog);
        when(flowController.getPerformanceTrackingPercentage()).thenReturn(0);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final RepositoryContext repositoryContext = mock(RepositoryContext.class);
        when(repositoryContext.isRelationshipAvailabilitySatisfied(0)).thenReturn(true);
        final FlowFileEventRepository flowFileEventRepository = mock(FlowFileEventRepository.class);
        when(repositoryContext.getFlowFileEventRepository()).thenReturn(flowFileEventRepository);
        when(contextFactory.newProcessContext(eq(processorNode), any(AtomicLong.class))).thenReturn(repositoryContext);

        return processorNode;
    }

    private Connectable createFullyMockedConnectable(final int maxConcurrentTasks,
                                                      final AtomicInteger invocationCount,
                                                      final CountDownLatch invocationLatch) {
        final Connectable connectable = mock(Connectable.class);
        when(connectable.getIdentifier()).thenReturn(COMPONENT_ID);
        when(connectable.getName()).thenReturn("TestProcessor");
        when(connectable.getMaxConcurrentTasks()).thenReturn(maxConcurrentTasks);
        when(connectable.getIncomingConnections()).thenReturn(Collections.emptyList());
        when(connectable.getRelationships()).thenReturn(Collections.emptySet());
        when(connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS)).thenReturn(100L);
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(100L));
        when(connectable.getYieldExpiration()).thenReturn(0L);
        when(connectable.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(connectable.isTriggerWhenEmpty()).thenReturn(true);
        when(connectable.isIsolated()).thenReturn(false);
        when(connectable.getRunDuration(TimeUnit.NANOSECONDS)).thenReturn(0L);
        when(connectable.isSessionBatchingSupported()).thenReturn(false);
        when(connectable.getScheduledState()).thenReturn(ScheduledState.RUNNING);

        final Processor runnableComponent = mock(Processor.class);
        when(connectable.getRunnableComponent()).thenReturn(runnableComponent);

        doAnswer(invocation -> {
            invocationCount.incrementAndGet();
            invocationLatch.countDown();
            return null;
        }).when(connectable).onTrigger(any(), any());

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getName()).thenReturn("RootGroup");
        when(processGroup.getParent()).thenReturn(null);
        when(connectable.getProcessGroup()).thenReturn(processGroup);

        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(stateManagerProvider.getStateManager(eq(COMPONENT_ID))).thenReturn(stateManager);
        when(flowController.getGarbageCollectionLog()).thenReturn(garbageCollectionLog);
        when(flowController.getPerformanceTrackingPercentage()).thenReturn(0);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final RepositoryContext repositoryContext = mock(RepositoryContext.class);
        when(repositoryContext.isRelationshipAvailabilitySatisfied(0)).thenReturn(true);
        final FlowFileEventRepository flowFileEventRepository = mock(FlowFileEventRepository.class);
        when(repositoryContext.getFlowFileEventRepository()).thenReturn(flowFileEventRepository);
        when(contextFactory.newProcessContext(eq(connectable), any(AtomicLong.class))).thenReturn(repositoryContext);

        return connectable;
    }
}
