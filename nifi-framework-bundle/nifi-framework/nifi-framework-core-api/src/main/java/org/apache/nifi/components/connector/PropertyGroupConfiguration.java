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

package org.apache.nifi.components.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PropertyGroupConfiguration {
    private final String propertyGroupName;
    private final Map<String, String> propertyValues;

    public PropertyGroupConfiguration(final String propertyGroupName, final Map<String, String> propertyValues) {
        this.propertyGroupName = propertyGroupName;
        this.propertyValues = new HashMap<>(propertyValues);
    }

    public String getPropertyGroupName() {
        return propertyGroupName;
    }

    public Map<String, String> getPropertyValues() {
        return propertyValues;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final PropertyGroupConfiguration that = (PropertyGroupConfiguration) other;
        return Objects.equals(propertyGroupName, that.propertyGroupName) && Objects.equals(propertyValues, that.propertyValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyGroupName, propertyValues);
    }

    @Override
    public String toString() {
        return "PropertyGroupConfiguration{" +
               "propertyGroupName='" + propertyGroupName + '\'' +
               ", propertyValues=" + propertyValues +
               '}';
    }
}
