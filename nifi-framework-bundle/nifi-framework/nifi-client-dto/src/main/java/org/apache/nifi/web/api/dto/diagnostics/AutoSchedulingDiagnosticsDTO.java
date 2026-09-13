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
package org.apache.nifi.web.api.dto.diagnostics;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

@XmlType(name = "autoSchedulingDiagnostics")
public class AutoSchedulingDiagnosticsDTO {
    private String executionMode;
    private Integer contextCeiling;
    private Integer selectedConcurrency;
    private Integer activeInvocations;
    private Long selectedRunDurationMillis;
    private String controllerPhase;
    private String limitingReason;
    private Double measuredThroughput;
    private Long observationDurationMillis;
    private String lastDecision;
    private Boolean insufficientEvidence;
    private Boolean measurementsSupported;

    @Schema(description = "Automatic scheduling execution mode")
    public String getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(final String executionMode) {
        this.executionMode = executionMode;
    }

    public Integer getContextCeiling() {
        return contextCeiling;
    }

    public void setContextCeiling(final Integer contextCeiling) {
        this.contextCeiling = contextCeiling;
    }

    public Integer getSelectedConcurrency() {
        return selectedConcurrency;
    }

    public void setSelectedConcurrency(final Integer selectedConcurrency) {
        this.selectedConcurrency = selectedConcurrency;
    }

    public Integer getActiveInvocations() {
        return activeInvocations;
    }

    public void setActiveInvocations(final Integer activeInvocations) {
        this.activeInvocations = activeInvocations;
    }

    public Long getSelectedRunDurationMillis() {
        return selectedRunDurationMillis;
    }

    public void setSelectedRunDurationMillis(final Long selectedRunDurationMillis) {
        this.selectedRunDurationMillis = selectedRunDurationMillis;
    }

    public String getControllerPhase() {
        return controllerPhase;
    }

    public void setControllerPhase(final String controllerPhase) {
        this.controllerPhase = controllerPhase;
    }

    public String getLimitingReason() {
        return limitingReason;
    }

    public void setLimitingReason(final String limitingReason) {
        this.limitingReason = limitingReason;
    }

    public Double getMeasuredThroughput() {
        return measuredThroughput;
    }

    public void setMeasuredThroughput(final Double measuredThroughput) {
        this.measuredThroughput = measuredThroughput;
    }

    public Long getObservationDurationMillis() {
        return observationDurationMillis;
    }

    public void setObservationDurationMillis(final Long observationDurationMillis) {
        this.observationDurationMillis = observationDurationMillis;
    }

    public String getLastDecision() {
        return lastDecision;
    }

    public void setLastDecision(final String lastDecision) {
        this.lastDecision = lastDecision;
    }

    public Boolean getInsufficientEvidence() {
        return insufficientEvidence;
    }

    public void setInsufficientEvidence(final Boolean insufficientEvidence) {
        this.insufficientEvidence = insufficientEvidence;
    }

    public Boolean getMeasurementsSupported() {
        return measurementsSupported;
    }

    public void setMeasurementsSupported(final Boolean measurementsSupported) {
        this.measurementsSupported = measurementsSupported;
    }
}
