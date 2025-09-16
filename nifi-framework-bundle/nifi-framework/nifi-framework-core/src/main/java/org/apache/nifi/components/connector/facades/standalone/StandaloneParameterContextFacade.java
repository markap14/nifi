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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.ParameterContextFacade;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

// TODO: Implement
public class StandaloneParameterContextFacade implements ParameterContextFacade {
    @Override
    public String setValue(final String parameterName, final String parameterValue, final boolean sensitive) {
        return "";
    }

    @Override
    public String getValue(final String parameterName) {
        return "";
    }

    @Override
    public Set<String> getDefinedParameterNames() {
        return Set.of();
    }

    @Override
    public boolean isSensitive(final String s) {
        return false;
    }

    @Override
    public void createAsset(final String parameterName, final InputStream inputStream) throws IOException {

    }
}
