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

import { ProcessGroupDrainRequest, ProcessGroupDrainRequestEntity } from '../state/process-group-drain';

/**
 * Test fixture factory for a drain of Process Group `pg-1` that is still draining, with 4 of its original
 * 10 FlowFiles left. Overrides shallow-merge onto the default fixture.
 */
export function createDrainRequest(overrides: Partial<ProcessGroupDrainRequest> = {}): ProcessGroupDrainRequest {
    return {
        id: 'drain-1',
        processGroupId: 'pg-1',
        uri: '../nifi-api/process-groups/pg-1/drain-requests/drain-1',
        state: 'DRAINING',
        prepared: true,
        originalCount: 10,
        originalSize: 1000,
        currentCount: 4,
        currentSize: 400,
        blockingComponents: [],
        ...overrides
    };
}

export function createDrainRequestEntity(
    overrides: Partial<ProcessGroupDrainRequest> = {}
): ProcessGroupDrainRequestEntity {
    return { drainRequest: createDrainRequest(overrides) };
}
