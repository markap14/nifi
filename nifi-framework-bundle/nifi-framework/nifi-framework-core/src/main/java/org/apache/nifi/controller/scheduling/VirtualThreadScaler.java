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
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.connectable.Connectable;

/**
 * Determines whether an auto-scheduled processor should scale up, scale down, or hold steady.
 * Implementations may examine queue depths, backpressure ratios, throughput history, CPU load,
 * or any other signal relevant to the scaling decision.
 */
interface VirtualThreadScaler {

    /**
     * Evaluates the current state of the given connectable and its scaling state, returning a
     * recommendation to scale up, scale down, or hold the current concurrency level.
     *
     * @param connectable  the processor being evaluated
     * @param scalingState the mutable scaling state for this processor
     * @return the scaling recommendation
     */
    ScalingRecommendation evaluate(Connectable connectable, ScalingState scalingState);
}
