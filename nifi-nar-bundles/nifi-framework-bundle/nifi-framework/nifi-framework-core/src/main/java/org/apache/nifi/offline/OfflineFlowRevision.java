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

package org.apache.nifi.offline;

import java.util.Date;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.nifi.registry.flow.VersionedProcessGroup;

import io.swagger.annotations.ApiModelProperty;

@XmlRootElement(name = "flowRevision")
public class OfflineFlowRevision {
    private VersionedProcessGroup flow;
    private String description;
    private Date timestamp;

    @ApiModelProperty("The new version of the flow")
    public VersionedProcessGroup getFlow() {
        return flow;
    }

    public void setFlow(VersionedProcessGroup flow) {
        this.flow = flow;
    }

    @ApiModelProperty("A description of the change")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty("The timestamp of when the change occurred")
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
