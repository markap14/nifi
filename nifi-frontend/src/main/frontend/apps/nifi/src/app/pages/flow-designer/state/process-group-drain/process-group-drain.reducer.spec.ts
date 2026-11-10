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

import { initialState, processGroupDrainReducer } from './process-group-drain.reducer';
import {
    deleteDrainRequest,
    deleteDrainRequestSuccess,
    drainRequestEnded,
    pollDrainRequestSuccess,
    resetProcessGroupDrainState,
    submitDrainRequest,
    submitDrainRequestSuccess
} from './process-group-drain.actions';
import { createDrainRequestEntity } from '../../testing/process-group-drain-fixture';

describe('processGroupDrainReducer', () => {
    it('should track a submitted drain through polling until it ends or is deleted', () => {
        let state = processGroupDrainReducer(initialState, submitDrainRequest({ request: { processGroupId: 'pg-1' } }));
        expect(state).toEqual({ ...initialState, processGroupId: 'pg-1', status: 'draining' });

        const submitted = createDrainRequestEntity({ currentCount: 10 });
        state = processGroupDrainReducer(
            state,
            submitDrainRequestSuccess({ response: { drainRequestEntity: submitted } })
        );
        expect(state.drainRequestId).toBe('drain-1');
        expect(state.drainRequest).toEqual(submitted.drainRequest);

        const polled = createDrainRequestEntity({ state: 'FAILED', failureReason: 'Could not enable service' });
        state = processGroupDrainReducer(state, pollDrainRequestSuccess({ response: { drainRequestEntity: polled } }));
        expect(state.drainRequest).toEqual(polled.drainRequest);
        expect(state.status).toBe('draining');

        // a drain that NiFi has ended keeps the last drain request that was polled, which is the final status of the drain
        const afterDrainEnded = processGroupDrainReducer(state, drainRequestEnded());
        expect(afterDrainEnded).toEqual({ ...state, status: 'ended' });

        // a poll that finds no drain, for example because NiFi restarted, clears the drain request so that no stale status is shown
        const afterDrainMissing = processGroupDrainReducer(
            state,
            pollDrainRequestSuccess({ response: { drainRequestEntity: {} } })
        );
        expect(afterDrainMissing.drainRequest).toBeNull();
        expect(afterDrainMissing.drainRequestId).toBe('drain-1');

        state = processGroupDrainReducer(state, deleteDrainRequest());
        expect(state.status).toBe('ending');

        // a DELETE that returns no drain request clears the drain request, because the final status of the drain is unknown
        const afterEmptyResponse = processGroupDrainReducer(
            state,
            deleteDrainRequestSuccess({ response: { drainRequestEntity: {} } })
        );
        expect(afterEmptyResponse.status).toBe('ended');
        expect(afterEmptyResponse.drainRequest).toBeNull();

        const deleted = createDrainRequestEntity({ state: 'FAILED', failureReason: 'Could not enable service' });
        state = processGroupDrainReducer(
            state,
            deleteDrainRequestSuccess({ response: { drainRequestEntity: deleted } })
        );
        expect(state.status).toBe('ended');
        expect(state.drainRequest).toEqual(deleted.drainRequest);
        expect(state.processGroupId).toBe('pg-1');
        expect(state.drainRequestId).toBe('drain-1');

        state = processGroupDrainReducer(state, resetProcessGroupDrainState());
        expect(state).toEqual(initialState);
    });
});
