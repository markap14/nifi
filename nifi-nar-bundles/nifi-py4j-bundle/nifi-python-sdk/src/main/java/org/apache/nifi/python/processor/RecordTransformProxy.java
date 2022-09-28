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

package org.apache.nifi.python.processor;

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
public class RecordTransformProxy extends PythonProcessorProxy {
    private final PythonProcessorBridge bridge;
    private volatile RecordTransform transform;


    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("Record Reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .required(true)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("Record Writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original FlowFile will be routed to this relationship when it has been successfully transformed")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("The original FlowFile will be routed to this relationship if it unable to be transformed for some reason")
        .build();
    private static final Set<Relationship> implicitRelationships = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(REL_ORIGINAL, REL_FAILURE)));


    public RecordTransformProxy(final PythonProcessorBridge bridge) {
        super(bridge);
        this.bridge = bridge;
        this.transform = (RecordTransform) bridge.getProcessorAdapter().getProcessor();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.addAll(super.getSupportedPropertyDescriptors());
        return properties;
    }

    @Override
    protected Set<Relationship> getImplicitRelationships() {
        return implicitRelationships;
    }

    public void reloadProcessor() {
        final boolean reloaded = bridge.reload();
        if (reloaded) {
            transform = (RecordTransform) bridge.getProcessorAdapter().getProcessor();
            getLogger().info("Successfully reloaded Processor");
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<RecordGroupingKey, DestinationTuple> destinationTuples = new HashMap<>();
        final AttributeMap attributeMap = new FlowFileAttributeMap(flowFile);

        Map<Relationship, List<FlowFile>> flowFilesPerRelationship;
        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            Record record;
            while ((record = reader.nextRecord()) != null) {
                final RecordTransformResult result = transform.transform(context, record, attributeMap);
                writeResult(result, destinationTuples, writerFactory, session, flowFile);
            }

            // Update FlowFile attributes, close Record Writers, and map FlowFiles to their appropriate relationships
            flowFilesPerRelationship = mapResults(destinationTuples, session);
        } catch (final Exception e) {
            getLogger().error("Failed to transform {}; routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);

            destinationTuples.values().forEach(tuple -> {
                session.remove(tuple.getFlowFile());

                try {
                    tuple.getWriter().close();
                } catch (final IOException ioe) {
                    getLogger().warn("Failed to close Record Writer for FlowFile created in this session", ioe);
                }
            });

            return;
        }

        // Transfer FlowFiles to the appropriate relationships.
        // This must be done outside of the try/catch because we need to close the InputStream before transferring the FlowFile
        flowFilesPerRelationship.forEach((rel, flowFiles) -> session.transfer(flowFiles, rel));
        session.transfer(flowFile, REL_ORIGINAL);
    }


    /**
     * Create mapping of each Relationship to all FlowFiles that go to that Relationship.
     * This gives us a way to efficiently transfer FlowFiles and allows us to ensure that we are able
     * to finish the Record Sets and close the Writers (flushing results, etc.) appropriately before
     * transferring any FlowFiles. This way, if there is any error, we can cleanup easily.
     *
     * @param destinationTuples a mapping of RecordGroupingKey (relationship and optional partition) to a DestinationTuple (FlowFile and RecordSetWriter)
     * @param session the process session
     * @return a mapping of all Relationships to which a FlowFile should be routed to those FlowFiles that are to be routed to the given Relationship
     *
     * @throws IOException if unable to create a RecordSetWriter
     */
    private Map<Relationship, List<FlowFile>> mapResults(final Map<RecordGroupingKey, DestinationTuple> destinationTuples, final ProcessSession session) throws IOException {
        final Map<Relationship, List<FlowFile>> flowFilesPerRelationship = new HashMap<>();
        for (final Map.Entry<RecordGroupingKey, DestinationTuple> entry : destinationTuples.entrySet()) {
            final DestinationTuple destinationTuple = entry.getValue();
            final RecordSetWriter writer = destinationTuple.getWriter();

            final WriteResult writeResult = writer.finishRecordSet();
            writer.close();

            // Create attribute map
            final Map<String, String> attributes = new HashMap<>();
            attributes.putAll(writeResult.getAttributes());
            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put("mime.type", writer.getMimeType());

            final RecordGroupingKey groupingKey = entry.getKey();
            final Map<String, Object> partition = groupingKey.getPartition();
            if (partition != null) {
                partition.forEach((key, value) -> attributes.put(key, Objects.toString(value)));
            }

            // Update the FlowFile and add to the appropriate Relationship and grouping
            final FlowFile outputFlowFile = session.putAllAttributes(destinationTuple.getFlowFile(), attributes);
            final Relationship destinationRelationship = new Relationship.Builder().name(groupingKey.getRelationship()).build();
            final List<FlowFile> flowFiles = flowFilesPerRelationship.computeIfAbsent(destinationRelationship, key -> new ArrayList<>());
            flowFiles.add(outputFlowFile);
        }

        return flowFilesPerRelationship;
    }

    /**
     * Writes the RecordTransformResult to the appropriate RecordSetWriter
     *
     * @param result the result to write out
     * @param destinationTuples a mapping of RecordGroupingKey (relationship and optional partition) to a DestinationTuple (FlowFile and RecordSetWriter)
     * @param writerFactory RecordSetWriterFactory to use for creating a RecordSetWriter if necessary
     * @param session the ProcessSession
     * @param originalFlowFile the original FlowFile
     *
     * @throws SchemaNotFoundException if unable to find the appropriate schema when attempting to create a new RecordSetWriter
     * @throws IOException if unable to create a new RecordSetWriter
     */
    private void writeResult(final RecordTransformResult result, final Map<RecordGroupingKey, DestinationTuple> destinationTuples, final RecordSetWriterFactory writerFactory,
                             final ProcessSession session, final FlowFile originalFlowFile) throws SchemaNotFoundException, IOException {

        final Record transformed = createRecord(result);
        if (transformed == null) {
            getLogger().debug("Received null result from RecordTransform; will not write result to output for {}", originalFlowFile);
            return;
        }

        // Get the DestinationTuple for the specified relationship
        final RecordGroupingKey key = new RecordGroupingKey(result.getRelationship(), result.getPartition());
        DestinationTuple destinationTuple = destinationTuples.get(key);
        if (destinationTuple == null) {
            final FlowFile destinationFlowFile = session.create(originalFlowFile);

            final RecordSetWriter writer;
            try {
                final OutputStream out = session.write(destinationFlowFile);
                final Map<String, String> originalAttributes = originalFlowFile.getAttributes();
                final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, transformed.getSchema());
                writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes);
                writer.beginRecordSet();
            } catch (final Exception e) {
                session.remove(destinationFlowFile);
                throw e;
            }

            destinationTuple = new DestinationTuple(destinationFlowFile, writer);
            destinationTuples.put(key, destinationTuple);
        }

        // Transform the result into a Record and write it out
        destinationTuple.getWriter().write(transformed);
    }


    private Record createRecord(final RecordTransformResult transformResult) {
        final Map<String, Object> values = transformResult.getRecord();
        if (values == null) {
            return null;
        }

        final RecordSchema schema = transformResult.getSchema();
        if (schema == null) {
            return DataTypeUtils.toRecord(values, "<root>");
        } else {
            return DataTypeUtils.toRecord(values, schema, "<root>");
        }
    }

    /**
     * A tuple representing the name of a Relationship to which a Record should be transferred and an optional Partition that may distinguish
     * a Record from other Records going to the same Relationship
     */
    private static class RecordGroupingKey {
        private final String relationship;
        private final Map<String, Object> partition;

        public RecordGroupingKey(final String relationship, final Map<String, Object> partition) {
            this.relationship = relationship;
            this.partition = partition;
        }

        public String getRelationship() {
            return relationship;
        }

        public Map<String, Object> getPartition() {
            return partition;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final RecordGroupingKey that = (RecordGroupingKey) o;
            return Objects.equals(relationship, that.relationship) && Objects.equals(partition, that.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relationship, partition);
        }
    }

    /**
     * A tuple of a FlowFile and the RecordSetWriter to use for writing to that FlowFile
     */
    private static class DestinationTuple {
        private final FlowFile flowFile;
        private final RecordSetWriter writer;

        public DestinationTuple(final FlowFile flowFile, final RecordSetWriter writer) {
            this.flowFile = flowFile;
            this.writer = writer;
        }

        public FlowFile getFlowFile() {
            return flowFile;
        }

        public RecordSetWriter getWriter() {
            return writer;
        }
    }
}
