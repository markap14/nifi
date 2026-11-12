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

import { createAction, props } from '@ngrx/store';
import { DrainRequestSuccess, SubmitDrainRequest, ViewDrainRequest } from './index';

const PROCESS_GROUP_DRAIN_PREFIX = '[Process Group Drain]';

export const processGroupDrainApiError = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} API Error`,
    props<{ error: string }>()
);

export const resetProcessGroupDrainState = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Reset State`);

export const promptDrainRequest = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Prompt Drain Request`,
    props<{ request: SubmitDrainRequest }>()
);

export const submitDrainRequest = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Submit Drain Request`,
    props<{ request: SubmitDrainRequest }>()
);

export const submitDrainRequestSuccess = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Submit Drain Request Success`,
    props<{ response: DrainRequestSuccess }>()
);

export const viewDrainRequest = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} View Drain Request`,
    props<{ request: ViewDrainRequest }>()
);

export const startPollingDrainRequest = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Start Polling Drain Request`);

export const pollDrainRequest = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Poll Drain Request`);

export const pollDrainRequestSuccess = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Poll Drain Request Success`,
    props<{ response: DrainRequestSuccess }>()
);

export const stopPollingDrainRequest = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Stop Polling Drain Request`);

export const drainRequestEnded = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Drain Request Ended`);

export const deleteDrainRequest = createAction(`${PROCESS_GROUP_DRAIN_PREFIX} Delete Drain Request`);

export const deleteDrainRequestSuccess = createAction(
    `${PROCESS_GROUP_DRAIN_PREFIX} Delete Drain Request Success`,
    props<{ response: DrainRequestSuccess }>()
);
