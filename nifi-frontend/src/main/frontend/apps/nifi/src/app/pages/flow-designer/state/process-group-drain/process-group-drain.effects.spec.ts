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

import { EventEmitter } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog } from '@angular/material/dialog';
import { Action } from '@ngrx/store';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { Observable, of, Subject, throwError } from 'rxjs';
import { ComponentType, YesNoDialog } from '@nifi/shared';
import { ProcessGroupDrainEffects } from './process-group-drain.effects';
import * as ProcessGroupDrainActions from './process-group-drain.actions';
import * as FlowActions from '../flow/flow.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { DrainBlockingComponent, ProcessGroupDrainRequestEntity } from './index';
import { selectDrainProcessGroupId, selectDrainRequestId } from './process-group-drain.selectors';
import { ProcessGroupDrainService } from '../../service/process-group-drain.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { DrainProcessGroupDialog } from '../../ui/canvas/items/process-group/drain-process-group-dialog/drain-process-group-dialog.component';
import { createDrainRequestEntity } from '../../testing/process-group-drain-fixture';

describe('ProcessGroupDrainEffects', () => {
    let action$: Subject<Action>;

    async function setup() {
        const yes = new Subject<void>();
        const cancelDrain = new EventEmitter<void>();
        const goToComponent = new EventEmitter<DrainBlockingComponent>();
        const drainDialogClosed$ = new Subject<void>();

        const yesNoDialogRef = { componentInstance: { yes } };
        const drainDialogRef = {
            componentInstance: {
                drainState$: undefined as unknown,
                cancelDrain,
                goToComponent
            },
            afterClosed: () => drainDialogClosed$.asObservable(),
            close: vi.fn()
        };
        const dialogOpen = vi.fn((component: unknown) => (component === YesNoDialog ? yesNoDialogRef : drainDialogRef));

        await TestBed.configureTestingModule({
            providers: [
                ProcessGroupDrainEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    selectors: [
                        { selector: selectDrainProcessGroupId, value: 'pg-1' },
                        { selector: selectDrainRequestId, value: 'drain-1' }
                    ]
                }),
                {
                    provide: ProcessGroupDrainService,
                    useValue: {
                        submitDrainRequest: vi.fn(),
                        getDrainRequest: vi.fn(),
                        deleteDrainRequest: vi.fn()
                    }
                },
                { provide: ErrorHelper, useValue: { getErrorString: vi.fn().mockReturnValue('Drain error') } },
                { provide: MatDialog, useValue: { open: dialogOpen } }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ProcessGroupDrainEffects);
        const processGroupDrainService = TestBed.inject(ProcessGroupDrainService) as unknown as {
            submitDrainRequest: ReturnType<typeof vi.fn>;
            getDrainRequest: ReturnType<typeof vi.fn>;
            deleteDrainRequest: ReturnType<typeof vi.fn>;
        };
        const store = TestBed.inject(MockStore);
        const dispatchSpy = vi.spyOn(store, 'dispatch');

        return {
            effects,
            processGroupDrainService,
            store,
            dispatchSpy,
            dialogOpen,
            yes,
            drainDialogRef,
            drainDialogClosed$
        };
    }

    function collect(effect$: Observable<unknown>): unknown[] {
        const emitted: unknown[] = [];
        effect$.subscribe((action) => emitted.push(action));
        return emitted;
    }

    beforeEach(() => {
        action$ = new Subject<Action>();
    });

    afterEach(() => {
        action$.complete();
        vi.useRealTimers();
        TestBed.resetTestingModule();
    });

    it('should ask for confirmation before submitting the drain request', async () => {
        const { effects, dialogOpen, yes, dispatchSpy } = await setup();
        collect(effects.promptDrainRequest$);

        action$.next(ProcessGroupDrainActions.promptDrainRequest({ request: { processGroupId: 'pg-1' } }));

        expect(dialogOpen).toHaveBeenCalledWith(YesNoDialog, expect.anything());
        expect(dispatchSpy).not.toHaveBeenCalled();

        yes.next();

        expect(dispatchSpy).toHaveBeenCalledWith(
            ProcessGroupDrainActions.submitDrainRequest({ request: { processGroupId: 'pg-1' } })
        );
    });

    it('should POST the drain request and report the response or the error', async () => {
        const { effects, processGroupDrainService } = await setup();
        const emitted = collect(effects.submitDrainRequest$);
        const drainRequestEntity = createDrainRequestEntity();

        processGroupDrainService.submitDrainRequest.mockReturnValueOnce(of(drainRequestEntity));
        action$.next(ProcessGroupDrainActions.submitDrainRequest({ request: { processGroupId: 'pg-1' } }));

        processGroupDrainService.submitDrainRequest.mockReturnValueOnce(
            throwError(() => new HttpErrorResponse({ status: 409 }))
        );
        action$.next(ProcessGroupDrainActions.submitDrainRequest({ request: { processGroupId: 'pg-1' } }));

        expect(processGroupDrainService.submitDrainRequest).toHaveBeenCalledWith('pg-1');
        expect(emitted).toEqual([
            ProcessGroupDrainActions.submitDrainRequestSuccess({ response: { drainRequestEntity } }),
            ProcessGroupDrainActions.processGroupDrainApiError({ error: 'Drain error' })
        ]);
    });

    it('should start polling after the POST, or DELETE right away when failed', async () => {
        const { effects } = await setup();
        const emitted = collect(effects.submitDrainRequestSuccess$);

        const draining = createDrainRequestEntity();
        action$.next(
            ProcessGroupDrainActions.submitDrainRequestSuccess({ response: { drainRequestEntity: draining } })
        );
        const failed = createDrainRequestEntity({ state: 'FAILED', failureReason: 'Failed to stop sources' });
        action$.next(ProcessGroupDrainActions.submitDrainRequestSuccess({ response: { drainRequestEntity: failed } }));
        expect(emitted).toEqual([
            ProcessGroupDrainActions.startPollingDrainRequest(),
            ProcessGroupDrainActions.deleteDrainRequest()
        ]);
    });

    it.each([
        {
            start: 'Start Polling',
            startAction: ProcessGroupDrainActions.startPollingDrainRequest(),
            stop: 'Stop Polling',
            stopAction: ProcessGroupDrainActions.stopPollingDrainRequest()
        },
        {
            start: 'Start Polling',
            startAction: ProcessGroupDrainActions.startPollingDrainRequest(),
            stop: 'Drain Request Ended',
            stopAction: ProcessGroupDrainActions.drainRequestEnded()
        },
        {
            start: 'View Drain',
            startAction: ProcessGroupDrainActions.viewDrainRequest({
                request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' }
            }),
            stop: 'Reset',
            stopAction: ProcessGroupDrainActions.resetProcessGroupDrainState()
        }
    ])('should poll every 2 seconds from $start until $stop', async ({ startAction, stopAction }) => {
        vi.useFakeTimers();
        const { effects } = await setup();
        const emitted = collect(effects.startPollingDrainRequest$);

        action$.next(startAction);

        vi.advanceTimersByTime(1999);
        expect(emitted).toEqual([]);
        vi.advanceTimersByTime(1);
        expect(emitted).toEqual([ProcessGroupDrainActions.pollDrainRequest()]);
        vi.advanceTimersByTime(2000);
        expect(emitted).toHaveLength(2);

        action$.next(stopAction);
        vi.advanceTimersByTime(10000);
        expect(emitted).toHaveLength(2);
    });

    it('should GET the drain request when polling or viewing a drain, but not once the ids are cleared', async () => {
        const { effects, processGroupDrainService, store } = await setup();
        const emitted = collect(effects.pollDrainRequest$);
        const drainRequestEntity = createDrainRequestEntity();

        // a GET that takes longer than the polling interval is not cancelled, and the polls made while it is outstanding are skipped
        const slowResponse$ = new Subject<ProcessGroupDrainRequestEntity>();
        processGroupDrainService.getDrainRequest.mockReturnValueOnce(slowResponse$);
        action$.next(
            ProcessGroupDrainActions.viewDrainRequest({
                request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' }
            })
        );
        action$.next(ProcessGroupDrainActions.pollDrainRequest());
        expect(processGroupDrainService.getDrainRequest).toHaveBeenCalledTimes(1);
        slowResponse$.next(drainRequestEntity);
        slowResponse$.complete();

        processGroupDrainService.getDrainRequest.mockReturnValueOnce(
            throwError(() => new HttpErrorResponse({ status: 404 }))
        );
        action$.next(ProcessGroupDrainActions.pollDrainRequest());

        expect(processGroupDrainService.getDrainRequest).toHaveBeenCalledWith('pg-1', 'drain-1');
        expect(processGroupDrainService.getDrainRequest).toHaveBeenCalledTimes(2);
        expect(emitted).toEqual([
            ProcessGroupDrainActions.pollDrainRequestSuccess({ response: { drainRequestEntity } }),
            ProcessGroupDrainActions.processGroupDrainApiError({ error: 'Drain error' })
        ]);

        // an API error clears the ids, so a poll that fires before the dialog has closed sends no GET
        store.overrideSelector(selectDrainRequestId, null);
        store.refreshState();
        action$.next(ProcessGroupDrainActions.pollDrainRequest());
        expect(processGroupDrainService.getDrainRequest).toHaveBeenCalledTimes(2);
        expect(emitted).toHaveLength(2);
    });

    it.each([
        { name: 'polling stops', stopAction: ProcessGroupDrainActions.stopPollingDrainRequest() },
        { name: 'the dialog closes', stopAction: ProcessGroupDrainActions.resetProcessGroupDrainState() }
    ])('should drop a GET that is still in flight when $name', async ({ stopAction }) => {
        const { effects, processGroupDrainService } = await setup();
        const emitted = collect(effects.pollDrainRequest$);
        const response$ = new Subject<ProcessGroupDrainRequestEntity>();

        processGroupDrainService.getDrainRequest.mockReturnValueOnce(response$);
        action$.next(ProcessGroupDrainActions.pollDrainRequest());
        action$.next(stopAction);
        response$.next({});

        expect(emitted).toEqual([]);
    });

    it('should keep polling while draining, and stop once the drain has failed or ended', async () => {
        const { effects } = await setup();
        const emitted = collect(effects.pollDrainRequestSuccess$);

        action$.next(
            ProcessGroupDrainActions.pollDrainRequestSuccess({
                response: { drainRequestEntity: createDrainRequestEntity() }
            })
        );
        expect(emitted).toEqual([]);

        // a failed drain is ended with a DELETE so that the dialog shows its final status
        action$.next(
            ProcessGroupDrainActions.pollDrainRequestSuccess({
                response: { drainRequestEntity: createDrainRequestEntity({ state: 'FAILED', failureReason: 'Failed' }) }
            })
        );
        expect(emitted).toEqual([ProcessGroupDrainActions.stopPollingDrainRequest()]);

        // a drain that NiFi ended, or that no longer exists because NiFi restarted, needs no DELETE
        action$.next(
            ProcessGroupDrainActions.pollDrainRequestSuccess({
                response: { drainRequestEntity: createDrainRequestEntity({ state: 'ENDED', currentCount: 0 }) }
            })
        );
        action$.next(ProcessGroupDrainActions.pollDrainRequestSuccess({ response: { drainRequestEntity: {} } }));
        expect(emitted).toEqual([
            ProcessGroupDrainActions.stopPollingDrainRequest(),
            ProcessGroupDrainActions.drainRequestEnded(),
            ProcessGroupDrainActions.drainRequestEnded()
        ]);
    });

    it('should DELETE the drain request after polling stops and reload the flow once the drain has ended', async () => {
        const { effects, processGroupDrainService } = await setup();
        const stopPollingEmitted = collect(effects.stopPollingDrainRequest$);
        const deleteEmitted = collect(effects.deleteDrainRequest$);
        const drainEndedEmitted = collect(effects.drainEnded$);
        const ended = createDrainRequestEntity({ state: 'ENDED' });

        processGroupDrainService.deleteDrainRequest.mockReturnValueOnce(of(ended));
        action$.next(ProcessGroupDrainActions.stopPollingDrainRequest());
        expect(stopPollingEmitted).toEqual([ProcessGroupDrainActions.deleteDrainRequest()]);

        action$.next(ProcessGroupDrainActions.deleteDrainRequest());
        expect(processGroupDrainService.deleteDrainRequest).toHaveBeenCalledWith('pg-1', 'drain-1');
        expect(deleteEmitted).toEqual([
            ProcessGroupDrainActions.deleteDrainRequestSuccess({ response: { drainRequestEntity: ended } })
        ]);

        action$.next(ProcessGroupDrainActions.deleteDrainRequestSuccess({ response: { drainRequestEntity: ended } }));
        action$.next(ProcessGroupDrainActions.drainRequestEnded());
        expect(drainEndedEmitted).toEqual([FlowActions.reloadFlow(), FlowActions.reloadFlow()]);

        // a DELETE that is still in flight when the dialog closes does not report anything
        const deleteResponse$ = new Subject<ProcessGroupDrainRequestEntity>();
        processGroupDrainService.deleteDrainRequest.mockReturnValueOnce(deleteResponse$);
        action$.next(ProcessGroupDrainActions.deleteDrainRequest());
        action$.next(ProcessGroupDrainActions.resetProcessGroupDrainState());
        deleteResponse$.next(ended);
        expect(deleteEmitted).toHaveLength(1);
    });

    it('should open the drain dialog and wire Cancel Drain, Go To, and closing to the store', async () => {
        const { effects, dialogOpen, drainDialogRef, drainDialogClosed$, dispatchSpy } = await setup();
        collect(effects.openDrainDialog$);

        action$.next(
            ProcessGroupDrainActions.viewDrainRequest({
                request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' }
            })
        );

        expect(dialogOpen).toHaveBeenCalledWith(
            DrainProcessGroupDialog,
            expect.objectContaining({ disableClose: true })
        );
        expect(drainDialogRef.componentInstance.drainState$).toBeDefined();

        drainDialogRef.componentInstance.cancelDrain.next();
        expect(dispatchSpy).toHaveBeenCalledWith(ProcessGroupDrainActions.stopPollingDrainRequest());

        drainDialogRef.componentInstance.goToComponent.next({
            id: 'port-1',
            groupId: 'pg-child',
            type: 'OUTPUT_PORT',
            name: 'Out',
            reason: 'DISABLED',
            queuedCount: 3
        });
        expect(drainDialogRef.close).toHaveBeenCalled();
        expect(dispatchSpy).toHaveBeenCalledWith(
            FlowActions.navigateToComponent({
                request: { id: 'port-1', type: ComponentType.OutputPort, processGroupId: 'pg-child' }
            })
        );

        drainDialogRef.componentInstance.goToComponent.next({
            id: 'remote-port-1',
            groupId: 'pg-child',
            remoteProcessGroupId: 'rpg-1',
            type: 'REMOTE_INPUT_PORT',
            name: 'Remote In',
            reason: 'INVALID',
            queuedCount: 2
        });
        expect(dispatchSpy).toHaveBeenCalledWith(
            FlowActions.navigateToComponent({
                request: { id: 'rpg-1', type: ComponentType.RemoteProcessGroup, processGroupId: 'pg-child' }
            })
        );

        drainDialogClosed$.next();
        expect(dispatchSpy).toHaveBeenCalledWith(ProcessGroupDrainActions.resetProcessGroupDrainState());
    });

    it('should close the drain dialog on an API error and show the error, and on a full screen error stop the workflow without ending the drain', async () => {
        vi.useFakeTimers();
        const { effects, drainDialogRef, drainDialogClosed$, processGroupDrainService, dispatchSpy } = await setup();
        collect(effects.openDrainDialog$);
        collect(effects.fullScreenError$);
        const submitEmitted = collect(effects.submitDrainRequest$);
        const pollingEmitted = collect(effects.startPollingDrainRequest$);
        const emitted = collect(effects.processGroupDrainApiError$);

        // closing the dialog reports that it closed, and the actions that the effects dispatch reach the effects, as they do through the store
        drainDialogRef.close.mockImplementation(() => drainDialogClosed$.next());
        dispatchSpy.mockImplementation((action: Action) => action$.next(action));
        const fullScreenError = ErrorActions.fullScreenError({
            errorDetail: {
                title: 'Unable to communicate with NiFi',
                message: 'Please ensure the application is running'
            }
        });

        action$.next(
            ProcessGroupDrainActions.submitDrainRequestSuccess({
                response: { drainRequestEntity: createDrainRequestEntity() }
            })
        );
        action$.next(ProcessGroupDrainActions.processGroupDrainApiError({ error: 'Drain error' }));

        expect(drainDialogRef.close).toHaveBeenCalledTimes(1);
        expect(emitted).toEqual([ErrorActions.snackBarError({ error: 'Drain error' })]);

        // when the connection to NiFi is lost, the HTTP interceptors show a full screen error and leave the failed request unanswered
        action$.next(
            ProcessGroupDrainActions.viewDrainRequest({
                request: { processGroupId: 'pg-1', drainRequestId: 'drain-1' }
            })
        );
        vi.advanceTimersByTime(2000);
        expect(pollingEmitted).toEqual([ProcessGroupDrainActions.pollDrainRequest()]);

        action$.next(fullScreenError);
        expect(drainDialogRef.close).toHaveBeenCalledTimes(2);
        expect(dispatchSpy).toHaveBeenLastCalledWith(ProcessGroupDrainActions.resetProcessGroupDrainState());
        vi.advanceTimersByTime(10000);
        expect(pollingEmitted).toHaveLength(1);

        // a POST that is still in flight when a full screen error is shown does not open the dialog once it completes
        const submitResponse$ = new Subject<ProcessGroupDrainRequestEntity>();
        processGroupDrainService.submitDrainRequest.mockReturnValueOnce(submitResponse$);
        action$.next(ProcessGroupDrainActions.submitDrainRequest({ request: { processGroupId: 'pg-1' } }));
        action$.next(fullScreenError);
        submitResponse$.next(createDrainRequestEntity());
        expect(submitEmitted).toEqual([]);

        expect(emitted).toHaveLength(1);
        expect(processGroupDrainService.deleteDrainRequest).not.toHaveBeenCalled();
    });
});
