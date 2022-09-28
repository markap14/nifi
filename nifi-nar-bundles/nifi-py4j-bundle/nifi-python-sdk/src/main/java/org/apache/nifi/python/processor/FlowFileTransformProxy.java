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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.Map;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
public class FlowFileTransformProxy extends PythonProcessorProxy {

    private final PythonProcessorBridge bridge;
    private volatile FlowFileTransform transform;


    public FlowFileTransformProxy(final PythonProcessorBridge bridge) {
        super(bridge);
        this.bridge = bridge;
        this.transform = (FlowFileTransform) bridge.getProcessorAdapter().getProcessor();
    }


    protected void reloadProcessor() {
        final boolean reloaded = bridge.reload();
        if (reloaded) {
            transform = (FlowFileTransform) bridge.getProcessorAdapter().getProcessor();
            getLogger().info("Successfully reloaded Processor");
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final FlowFileTransformResult result;
        try (final StandardInputFlowFile inputFlowFile = new StandardInputFlowFile(session, flowFile)) {
            result = transform.transform(context, inputFlowFile);
        } catch (IOException e) {
            throw new ProcessException("Could not read FlowFile contents");
        }

        final Map<String, String> attributes = result.getAttributes();
        if (attributes != null) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        final byte[] contents = result.getContents();
        if (contents != null) {
            flowFile = session.write(flowFile, out -> out.write(contents));
        }

        final String relationshipName = result.getRelationship();
        final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
        session.transfer(flowFile, relationship);
    }

}
