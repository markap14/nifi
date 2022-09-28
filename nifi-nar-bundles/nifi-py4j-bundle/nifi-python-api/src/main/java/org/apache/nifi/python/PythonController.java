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

import org.apache.nifi.python.processor.PythonProcessorAdapter;

import java.util.List;

public interface PythonController {
    String ping();

    List<PythonProcessorDetails> getProcessorTypes();

    void discoverExtensions(List<String> directories, String workDirectory);

    PythonProcessorAdapter createProcessor(String type, String version, String workDirectory);

    void reloadProcessor(String type, String version, String workDirectory);

    void setControllerServiceTypeLookup(ControllerServiceTypeLookup lookup);

    List<String> getProcessorDependencies(String processorType);

    String getModuleFile(String processorType, String version);

}
