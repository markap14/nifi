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
package org.apache.nifi.controller.scheduling.auto;

import org.apache.nifi.controller.scheduling.SchedulingSettings;

import java.util.concurrent.TimeUnit;

/**
 * Chooses how many concurrent processor calls should be allowed.
 *
 * <p>The controller starts with one concurrent call. It watches completed work, processor demand, processor
 * occupancy, blocked time, failures, CPU use, garbage collection, and the shared thread budget. It changes the
 * setting only after there is enough information to make a useful comparison.</p>
 *
 * <p>When there is sustained demand and the existing calls are busy, the controller tests a higher number of
 * concurrent calls. A higher number is kept only when it produces a clear increase in completed work. The first
 * increase is one call. Later increases are larger, but never exceed the processor ceiling or the shared thread
 * budget. A failed increase waits before it is tried again.</p>
 *
 * <p>The controller also looks for a lower number of calls. If the processor is mostly idle or blocked for several
 * seconds, it removes one unused call immediately. At regular intervals it tests one fewer call even when the
 * existing calls are busy. That lower setting is kept only when completed work stays close to the best confirmed
 * rate and the input backlog does not grow. A failed lower-setting test leaves the current setting in place.</p>
 *
 * <p>Every test is temporary. The controller measures the current setting, applies the possible change, and measures
 * the changed setting. It then either keeps the change or returns to the previous setting. Tests are abandoned when
 * there is too little work, the processor fails, the processor becomes blocked, or the available resources change.
 * Resource limits stop new increases but do not remove productive calls merely because the machine is busy.</p>
 *
 * <p>The run duration is fixed when this controller is created. Batching support uses a 25 millisecond duration;
 * processors without batching use no added duration.</p>
 */
public class StandardAutoSchedulingController {
    private static final long MINIMUM_PROBE_NANOS = TimeUnit.SECONDS.toNanos(6);
    private static final long MAXIMUM_PROBE_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long GROWTH_COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long REDUCTION_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long IDLE_REDUCTION_NANOS = TimeUnit.SECONDS.toNanos(3);
    private static final double THROUGHPUT_TOLERANCE = 0.05D;

    private final int contextCeiling;
    private final long runDurationNanos;
    private Probe probe;
    private double baselineWork;
    private double baselineNanos;
    private long averageInvocationNanos;
    private long nextGrowthNanos;
    private long nextReductionNanos;
    private long idleNanos;

    public StandardAutoSchedulingController(final int contextCeiling, final boolean batchingSupported, final boolean serial) {
        this.contextCeiling = serial ? 1 : contextCeiling;
        runDurationNanos = batchingSupported ? TimeUnit.MILLISECONDS.toNanos(25) : 0L;
    }

    /**
     * Reviews the latest observations and decides whether the current setting should change.
     *
     * <p>Growth requires recent demand, available input, busy workers, and no sign that the processor or node is
     * limited by another resource. A growth test must show a clear improvement. Reduction is allowed after a period
     * of low use or on the regular reduction schedule. A reduction may be kept when it preserves nearly all of the
     * confirmed work rate without causing the input backlog to grow.</p>
     *
     * @param observation observations for this processor
     * @param node observations for the node and its shared thread budget
     * @return the current setting or a setting to apply
     */
    public AutoSchedulingDecision evaluate(final AutoSchedulingObservation observation, final NodeSchedulingObservation node) {
        final SchedulingSettings settings = observation.appliedSettings();
        final long nowNanos = observation.timestampNanos();
        final int limit = Math.max(1, Math.min(contextCeiling, node.globalBudget()));
        if (settings.concurrentTasks() > limit || settings.runDurationNanos() != runDurationNanos) {
            reset();
            return decision(Math.min(settings.concurrentTasks(), limit), AutoDecisionReason.SETTINGS_CLAMPED, true);
        }

        if (observation.primaryNodeChanged()) {
            reset();
        }

        if (observation.completedInvocations() > 0L && observation.invocationNanos() > 0L) {
            averageInvocationNanos = observation.invocationNanos() / observation.completedInvocations();
        }

        final boolean resourceLimited = node.resourceGuardEngaged() || node.smoothedCpuLoad() >= 0.90D || node.garbageCollectionOverhead() >= 0.10D;
        final boolean admissionLimited = node.globalUtilization() >= 0.90D && node.globalContention() > 0.10D;
        final boolean blocked = observation.outputBlockedNanos() + observation.yieldedNanos() >= observation.durationNanos() / 2;
        final boolean demand = observation.inputBacklogged() || observation.sourceDemand();
        if (probe != null) {
            return evaluateProbe(observation);
        }

        baselineWork += observation.committedFlowFiles();
        baselineNanos += observation.durationNanos();
        // Average work over elapsed time, including quiet samples between slow completions. Fade old measurements without discarding the baseline.
        final long baselineWindowNanos = Math.max(MINIMUM_PROBE_NANOS, Math.min(MAXIMUM_PROBE_NANOS, averageInvocationNanos * 2));
        if (baselineNanos > baselineWindowNanos * 2) {
            baselineWork *= 0.5D;
            baselineNanos *= 0.5D;
        }

        if (nextReductionNanos == 0L) {
            nextReductionNanos = nowNanos + REDUCTION_INTERVAL_NANOS;
        }

        // Release excess workers when a processor stays mostly idle, including processors that intermittently consume incoming batches.
        final boolean idle = observation.activeOccupancy() < 0.5D;
        idleNanos = idle || blocked ? idleNanos + observation.durationNanos() : 0L;
        if (settings.concurrentTasks() > 1 && idleNanos >= IDLE_REDUCTION_NANOS) {
            idleNanos = 0L;
            return decision(settings.concurrentTasks() - 1, AutoDecisionReason.DEMAND_LIMITED, true);
        }

        if (observation.failures() > 0L) {
            return decision(settings.concurrentTasks(), AutoDecisionReason.PROCESSOR_FAILURE, false);
        }

        // Periodic downward probes discover equal-throughput configurations that use fewer tasks, even when all workers stay busy.
        if (settings.concurrentTasks() > 1 && nowNanos >= nextReductionNanos) {
            return startProbe(observation, settings.concurrentTasks() - 1);
        }

        // Saturation prevents further growth; reducing productive capacity merely because it filled the CPU would undo successful scaling.
        if (resourceLimited || admissionLimited) {
            return decision(settings.concurrentTasks(), resourceLimited ? AutoDecisionReason.RESOURCE_GUARD : AutoDecisionReason.GLOBAL_ADMISSION_CONSTRAINED, false);
        }

        // Start with one speculative worker even before the first completion. Larger steps then make useful gains distinguishable from commit-rate variation.
        final boolean baselineEstablished = settings.concurrentTasks() == 1 || baselineNanos >= baselineWindowNanos;
        if (settings.concurrentTasks() < limit && nowNanos >= nextGrowthNanos && baselineEstablished && demand && observation.inputReady() && !blocked && observation.activeOccupancy() >= 0.75D) {
            return startProbe(observation, Math.min(limit, settings.concurrentTasks() + Math.max(1, settings.concurrentTasks() / 2)));
        }

        return decision(settings.concurrentTasks(), demand ? AutoDecisionReason.COOLDOWN : AutoDecisionReason.DEMAND_LIMITED, false);
    }

    private AutoSchedulingDecision startProbe(final AutoSchedulingObservation observation, final int candidateConcurrency) {
        final long windowNanos = Math.max(MINIMUM_PROBE_NANOS, Math.min(MAXIMUM_PROBE_NANOS, averageInvocationNanos * 2));
        final double baselineRate = baselineWork * TimeUnit.SECONDS.toNanos(1) / Math.max(1D, baselineNanos);
        probe = new Probe(observation.appliedSettings().concurrentTasks(), candidateConcurrency, baselineRate, observation.timestampNanos(), windowNanos);
        return decision(candidateConcurrency, AutoDecisionReason.CANDIDATE_STARTED, true);
    }

    private AutoSchedulingDecision evaluateProbe(final AutoSchedulingObservation observation) {
        final long nowNanos = observation.timestampNanos();
        final boolean increasing = probe.candidateConcurrency > probe.previousConcurrency;
        probe.work += observation.committedFlowFiles();
        probe.durationNanos += observation.durationNanos();
        probe.invocations += observation.completedInvocations();
        probe.backlogGrowth += observation.localInputBacklogTrend();
        final boolean expired = nowNanos - probe.startedNanos >= MAXIMUM_PROBE_NANOS;
        final boolean failed = observation.failures() > 0L;
        final boolean enoughEvidence = probe.durationNanos >= probe.minimumDurationNanos && (probe.work > 0L || probe.invocations > 0L);
        if (!failed && !expired && !enoughEvidence) {
            return decision(probe.candidateConcurrency, AutoDecisionReason.INSUFFICIENT_EVIDENCE, false);
        }

        final double candidateRate = probe.work * (double) TimeUnit.SECONDS.toNanos(1) / Math.max(1L, probe.durationNanos);
        // One task offers a smaller relative gain at high concurrency; the tolerance must remain below that potential gain.
        final double tolerance = Math.min(THROUGHPUT_TOLERANCE, 0.5D / probe.previousConcurrency);
        final boolean improved = candidateRate > probe.previousRate * (1D + tolerance);
        final boolean equivalent = candidateRate >= probe.previousRate * (1D - tolerance);
        // Empty input alone does not demonstrate a gain: downstream processors routinely drain incoming batches with fewer workers.
        final boolean accepted = !failed && enoughEvidence && (increasing ? improved : equivalent && probe.backlogGrowth <= 0D);
        final int selectedConcurrency = accepted ? probe.candidateConcurrency : probe.previousConcurrency;
        if (accepted) {
            baselineWork = probe.work;
            baselineNanos = probe.durationNanos;
        }

        nextGrowthNanos = accepted && increasing ? nowNanos : nowNanos + GROWTH_COOLDOWN_NANOS;
        if (accepted || !increasing) {
            nextReductionNanos = nowNanos + (accepted && !increasing ? IDLE_REDUCTION_NANOS : REDUCTION_INTERVAL_NANOS);
        }

        probe = null;
        idleNanos = 0L;
        return decision(selectedConcurrency, accepted ? AutoDecisionReason.CANDIDATE_ACCEPTED : AutoDecisionReason.CANDIDATE_REJECTED,
                selectedConcurrency != observation.appliedSettings().concurrentTasks());
    }

    public void reset() {
        probe = null;
        baselineWork = 0D;
        baselineNanos = 0D;
        averageInvocationNanos = 0L;
        nextGrowthNanos = 0L;
        nextReductionNanos = 0L;
        idleNanos = 0L;
    }

    private AutoSchedulingDecision decision(final int concurrency, final AutoDecisionReason reason, final boolean applySettings) {
        return new AutoSchedulingDecision(new SchedulingSettings(concurrency, runDurationNanos), probe == null ? AutoControllerPhase.HOLD : AutoControllerPhase.TRIAL, reason, applySettings);
    }

    private static class Probe {
        private final int previousConcurrency;
        private final int candidateConcurrency;
        private final double previousRate;
        private final long startedNanos;
        private final long minimumDurationNanos;
        private long work;
        private long durationNanos;
        private long invocations;
        private double backlogGrowth;

        private Probe(final int previousConcurrency, final int candidateConcurrency, final double previousRate, final long startedNanos, final long minimumDurationNanos) {
            this.previousConcurrency = previousConcurrency;
            this.candidateConcurrency = candidateConcurrency;
            this.previousRate = previousRate;
            this.startedNanos = startedNanos;
            this.minimumDurationNanos = minimumDurationNanos;
        }
    }
}
