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

package org.apache.nifi.python;

import org.apache.nifi.python.processor.PythonProcessorBridge;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PythonBridge {

    void initialize(PythonBridgeInitializationContext context);

    void start() throws IOException;

    void shutdown();

    void ping() throws IOException;

    List<PythonProcessorDetails> getProcessorTypes();

    /**
     * @return a mapping of Processor type to the number of Python Processes currently running that processor type
     */
    Map<String, Integer> getProcessCountsPerType();

    /**
     * In order to allow the Python Process to interact with Java objects, we must have some way for the Python process
     * to identify the object. This mapping between Object ID and object is held within a 'Java Object Bindings'.
     * This determines the number of objects of each type that are currently bound for each Python Process.
     *
     * @return a mapping of class names to the number of objects of that type that are currently bound for each Process
     */
    List<BoundObjectCounts> getBoundObjectCounts();

    List<String> getProcessorDependencies(String processorType);

    void discoverExtensions();

    PythonProcessorBridge createProcessor(String identifier, String type, String version, boolean preferIsolatedProcess);

    void onProcessorRemoved(String identifier, String type, String version);
}
