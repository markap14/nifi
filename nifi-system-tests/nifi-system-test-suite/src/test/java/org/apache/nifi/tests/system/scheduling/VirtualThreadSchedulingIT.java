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
package org.apache.nifi.tests.system.scheduling;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VirtualThreadSchedulingIT extends NiFiSystemIT {

    @Test
    public void testBasicVirtualThreadScheduling() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), 1);

        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());
        assertTrue(getConnectionQueueSize(connection.getId()) >= 1);
    }

    @Test
    public void testAutoStrategyViaRestApi() throws NiFiClientException, IOException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");

        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setSchedulingStrategy("AUTO");
        getClientUtil().updateProcessorConfig(generate, config);

        final ProcessorEntity updated = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("AUTO", updated.getComponent().getConfig().getSchedulingStrategy());
    }

    @Test
    public void testStoredValuePreservedWhenExceedingSystemMax() throws NiFiClientException, IOException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");

        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setConcurrentlySchedulableTaskCount(100);
        getClientUtil().updateProcessorConfig(generate, config);

        final ProcessorEntity updated = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals(100, updated.getComponent().getConfig().getConcurrentlySchedulableTaskCount().intValue());
    }

    @Test
    public void testStopAndRestartPreservesAutoStrategy() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");

        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setSchedulingStrategy("AUTO");
        getClientUtil().updateProcessorConfig(generate, config);

        ProcessorEntity current = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("AUTO", current.getComponent().getConfig().getSchedulingStrategy());

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessor(generate);
        Thread.sleep(1000);
        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        current = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("AUTO", current.getComponent().getConfig().getSchedulingStrategy());

        getClientUtil().startProcessor(current);
        Thread.sleep(1000);
        getClientUtil().stopProcessor(current);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        current = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("AUTO", current.getComponent().getConfig().getSchedulingStrategy());
    }

    @Test
    public void testAutoToTimerDrivenToggle() throws NiFiClientException, IOException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");

        final ProcessorConfigDTO autoConfig = new ProcessorConfigDTO();
        autoConfig.setSchedulingStrategy("AUTO");
        getClientUtil().updateProcessorConfig(generate, autoConfig);

        ProcessorEntity updated = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("AUTO", updated.getComponent().getConfig().getSchedulingStrategy());

        final ProcessorConfigDTO timerConfig = new ProcessorConfigDTO();
        timerConfig.setSchedulingStrategy("TIMER_DRIVEN");
        timerConfig.setConcurrentlySchedulableTaskCount(4);
        timerConfig.setRunDurationMillis(25L);
        getClientUtil().updateProcessorConfig(updated, timerConfig);

        updated = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        assertEquals("TIMER_DRIVEN", updated.getComponent().getConfig().getSchedulingStrategy());
        assertEquals(4, updated.getComponent().getConfig().getConcurrentlySchedulableTaskCount().intValue());
        assertEquals(25L, updated.getComponent().getConfig().getRunDurationMillis().longValue());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testAutoModeSustainedThroughputOneMillionFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        final int targetProcessedCount = 1_000_000;

        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorConfigDTO generateConfig = new ProcessorConfigDTO();
        generateConfig.setSchedulingStrategy("AUTO");
        generateConfig.setSchedulingPeriod("0 sec");
        generateConfig.setProperties(Map.of("Batch Size", "1000", "File Size", "0B"));
        generate = getClientUtil().updateProcessorConfig(generate, generateConfig);

        ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ProcessorConfigDTO terminateConfig = new ProcessorConfigDTO();
        terminateConfig.setSchedulingStrategy("AUTO");
        terminate = getClientUtil().updateProcessorConfig(terminate, terminateConfig);

        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(terminate.getId());

        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(terminate);

        final String connectionId = connection.getId();
        waitFor(() -> {
            final ConnectionStatusEntity statusEntity = getNifiClient().getFlowClient().getConnectionStatus(connectionId, true);
            final ConnectionStatusSnapshotDTO snapshot = statusEntity.getConnectionStatus().getAggregateSnapshot();
            final int flowFilesOut = snapshot.getFlowFilesOut();
            return flowFilesOut >= targetProcessedCount;
        }, 1000L);
    }

    @Test
    public void testTriggerSeriallyProcessorCapsConcurrentTasksAtOne() throws NiFiClientException, IOException {
        final ProcessorEntity merge = getClientUtil().createProcessor("ConcatenateRangeOfFlowFiles");

        final ProcessorConfigDTO timerConfig = new ProcessorConfigDTO();
        timerConfig.setSchedulingStrategy("TIMER_DRIVEN");
        timerConfig.setConcurrentlySchedulableTaskCount(8);
        getClientUtil().updateProcessorConfig(merge, timerConfig);

        final ProcessorEntity updated = getNifiClient().getProcessorClient().getProcessor(merge.getId());
        assertEquals(1, updated.getComponent().getConfig().getConcurrentlySchedulableTaskCount().intValue(),
                "@TriggerSerially processor must not store concurrent task counts above 1");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testAutoModeProcessesOneMillionFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        final int targetFlowFileCount = 1_000_000;

        ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorConfigDTO generateConfig = new ProcessorConfigDTO();
        generateConfig.setSchedulingStrategy("AUTO");
        generateConfig.setSchedulingPeriod("0 sec");
        generateConfig.setProperties(Map.of("Batch Size", "1000", "File Size", "0B"));
        generate = getClientUtil().updateProcessorConfig(generate, generateConfig);

        ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ProcessorConfigDTO terminateConfig = new ProcessorConfigDTO();
        terminateConfig.setSchedulingStrategy("AUTO");
        terminate = getClientUtil().updateProcessorConfig(terminate, terminateConfig);

        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");
        getClientUtil().updateConnectionBackpressure(connection, targetFlowFileCount * 2L, Long.MAX_VALUE);

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(terminate.getId());

        getClientUtil().startProcessor(generate);
        waitForMinQueueCount(connection.getId(), targetFlowFileCount);

        getClientUtil().startProcessor(terminate);
        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        waitForQueueCount(connection.getId(), 0);
    }
}
