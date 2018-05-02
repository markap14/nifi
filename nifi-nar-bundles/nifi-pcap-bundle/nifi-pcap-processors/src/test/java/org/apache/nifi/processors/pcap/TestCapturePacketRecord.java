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

package org.apache.nifi.processors.pcap;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IllegalRawDataException;
import org.pcap4j.packet.Packet;

public class TestCapturePacketRecord {


    @Test
    public void testRecordCaptured() throws IOException, IllegalRawDataException, InitializationException {
        final byte[] rawData = Files.readAllBytes(Paths.get("src/test/resources/1.packet"));
        final Packet packet = EthernetPacket.newPacket(rawData, 0, rawData.length);
        final Queue<Packet> packets = new LinkedBlockingQueue<>();
        packets.add(packet);

        final TestRunner runner = TestRunners.newTestRunner(new CapturePacketRecord() {
            @Override
            public void setup(ProcessContext context) throws UnknownHostException, PcapNativeException {
                // Do not start listening because this is a unit test and listening would require elevated permissions
            }

            @Override
            protected Packet getPacket() {
                return packets.poll();
            }
        });

        runner.setProperty(CapturePacketRecord.ADDRESS, "localhost");
        runner.setProperty(CapturePacketRecord.RECORD_WRITER, "writer");

        final CapturingRecordSetWriterFactory writer = new CapturingRecordSetWriterFactory();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);

        runner.run();

        runner.assertAllFlowFilesTransferred(CapturePacketRecord.SUCCESS, 1);

        final List<Record> recordsWritten = writer.getRecordsWritten();
        assertEquals(1, recordsWritten.size());

        final Record record = recordsWritten.get(0);
        System.out.println(record);

    }

    private static class CapturingRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        private List<Record> recordsWritten = new ArrayList<>();

        public List<Record> getRecordsWritten() {
            return Collections.unmodifiableList(recordsWritten);
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out) throws SchemaNotFoundException, IOException {
            return new RecordSetWriter() {

                @Override
                public WriteResult write(Record record) throws IOException {
                    recordsWritten.add(record);
                    return WriteResult.of(recordsWritten.size(), Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return null;
                }

                @Override
                public void flush() throws IOException {
                }

                @Override
                public void close() throws IOException {
                }

                @Override
                public WriteResult write(RecordSet recordSet) throws IOException {
                    WriteResult result = null;

                    Record record;
                    while ((record = recordSet.next()) != null) {
                        result = write(record);
                    }

                    if (result == null) {
                        return WriteResult.EMPTY;
                    }

                    return result;
                }

                @Override
                public void beginRecordSet() throws IOException {
                }

                @Override
                public WriteResult finishRecordSet() throws IOException {
                    return WriteResult.of(recordsWritten.size(), Collections.emptyMap());
                }
            };
        }

    }
}
