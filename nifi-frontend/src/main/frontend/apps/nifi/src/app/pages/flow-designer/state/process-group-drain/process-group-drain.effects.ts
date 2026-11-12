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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import {
    asyncScheduler,
    catchError,
    exhaustMap,
    filter,
    interval,
    map,
    of,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs';
import { ComponentType, isDefinedAndNotNull, MEDIUM_DIALOG, SMALL_DIALOG, YesNoDialog } from '@nifi/shared';
import * as ProcessGroupDrainActions from './process-group-drain.actions';
import * as FlowActions from '../flow/flow.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { NiFiState } from '../../../../state';
import { DrainBlockingComponentType } from './index';
import {
    selectDrainProcessGroupId,
    selectDrainRequestId,
    selectProcessGroupDrainState
} from './process-group-drain.selectors';
import { ProcessGroupDrainService } from '../../service/process-group-drain.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { DrainProcessGroupDialog } from '../../ui/canvas/items/process-group/drain-process-group-dialog/drain-process-group-dialog.component';

const COMPONENT_TYPES: Record<DrainBlockingComponentType, ComponentType> = {
    PROCESSOR: ComponentType.Processor,
    INPUT_PORT: ComponentType.InputPort,
    OUTPUT_PORT: ComponentType.OutputPort,
    REMOTE_INPUT_PORT: ComponentType.RemoteProcessGroup
};

/**
 * NiFi ends a drain itself once no FlowFiles are left, so polling stops once a poll finds that the drain is no longer
 * draining. A drain that failed is ended with a DELETE so that it stops on every node of a cluster.
 */
@Injectable()
export class ProcessGroupDrainEffects {
    private actions$ = inject(Actions);
    private store = inject<Store<NiFiState>>(Store);
    private processGroupDrainService = inject(ProcessGroupDrainService);
    private errorHelper = inject(ErrorHelper);
    private dialog = inject(MatDialog);

    private drainDialogRef: MatDialogRef<DrainProcessGroupDialog> | undefined;

    promptDrainRequest$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProcessGroupDrainActions.promptDrainRequest),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        ...SMALL_DIALOG,
                        data: {
                            title: 'Drain Process Group',
                            message:
                                'Draining stops the components that bring data into this Process Group and starts all other components so that the queued data can flow out. Are you sure you want to drain this Process Group?'
                        }
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(ProcessGroupDrainActions.submitDrainRequest({ request }));
                    });
                })
            ),
        { dispatch: false }
    );

    submitDrainRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.submitDrainRequest),
            map((action) => action.request),
            switchMap((request) =>
                this.processGroupDrainService.submitDrainRequest(request.processGroupId).pipe(
                    map((response) =>
                        ProcessGroupDrainActions.submitDrainRequestSuccess({
                            response: { drainRequestEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ProcessGroupDrainActions.processGroupDrainApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    ),
                    // the drain still begins in NiFi, but the dialog must not open over the full screen error
                    takeUntil(this.actions$.pipe(ofType(ErrorActions.fullScreenError)))
                )
            )
        )
    );

    submitDrainRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.submitDrainRequestSuccess),
            map((action) => action.response.drainRequestEntity.drainRequest),
            map((drainRequest) =>
                drainRequest?.state === 'FAILED'
                    ? ProcessGroupDrainActions.deleteDrainRequest()
                    : ProcessGroupDrainActions.startPollingDrainRequest()
            )
        )
    );

    openDrainDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ProcessGroupDrainActions.submitDrainRequestSuccess, ProcessGroupDrainActions.viewDrainRequest),
                tap(() => {
                    const dialogReference = this.dialog.open(DrainProcessGroupDialog, {
                        ...MEDIUM_DIALOG,
                        disableClose: true,
                        autoFocus: false
                    });
                    this.drainDialogRef = dialogReference;

                    dialogReference.componentInstance.drainState$ = this.store.select(selectProcessGroupDrainState);

                    dialogReference.componentInstance.cancelDrain
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe(() => {
                            this.store.dispatch(ProcessGroupDrainActions.stopPollingDrainRequest());
                        });

                    dialogReference.componentInstance.goToComponent
                        .pipe(takeUntil(dialogReference.afterClosed()))
                        .subscribe((component) => {
                            dialogReference.close();
                            // a Remote Process Group Input Port is shown on the canvas as part of its Remote Process Group
                            this.store.dispatch(
                                FlowActions.navigateToComponent({
                                    request: {
                                        id: component.remoteProcessGroupId ?? component.id,
                                        type: COMPONENT_TYPES[component.type],
                                        processGroupId: component.groupId
                                    }
                                })
                            );
                        });

                    dialogReference.afterClosed().subscribe(() => {
                        this.drainDialogRef = undefined;
                        this.store.dispatch(ProcessGroupDrainActions.resetProcessGroupDrainState());
                    });
                })
            ),
        { dispatch: false }
    );

    startPollingDrainRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.startPollingDrainRequest, ProcessGroupDrainActions.viewDrainRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(
                        this.actions$.pipe(
                            ofType(
                                ProcessGroupDrainActions.stopPollingDrainRequest,
                                ProcessGroupDrainActions.drainRequestEnded,
                                ProcessGroupDrainActions.resetProcessGroupDrainState
                            )
                        )
                    )
                )
            ),
            map(() => ProcessGroupDrainActions.pollDrainRequest())
        )
    );

    pollDrainRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.pollDrainRequest, ProcessGroupDrainActions.viewDrainRequest),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId).pipe(isDefinedAndNotNull()),
                this.store.select(selectDrainRequestId).pipe(isDefinedAndNotNull())
            ]),
            exhaustMap(([, processGroupId, drainRequestId]) =>
                this.processGroupDrainService.getDrainRequest(processGroupId, drainRequestId).pipe(
                    map((response) =>
                        ProcessGroupDrainActions.pollDrainRequestSuccess({
                            response: { drainRequestEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ProcessGroupDrainActions.processGroupDrainApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    ),
                    // a GET that is still in flight when polling stops or the dialog closes must not report anything
                    takeUntil(
                        this.actions$.pipe(
                            ofType(
                                ProcessGroupDrainActions.stopPollingDrainRequest,
                                ProcessGroupDrainActions.resetProcessGroupDrainState
                            )
                        )
                    )
                )
            )
        )
    );

    pollDrainRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.pollDrainRequestSuccess),
            map((action) => action.response.drainRequestEntity.drainRequest),
            filter((drainRequest) => !drainRequest || drainRequest.state !== 'DRAINING'),
            map((drainRequest) =>
                drainRequest?.state === 'FAILED'
                    ? ProcessGroupDrainActions.stopPollingDrainRequest()
                    : ProcessGroupDrainActions.drainRequestEnded()
            )
        )
    );

    stopPollingDrainRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.stopPollingDrainRequest),
            map(() => ProcessGroupDrainActions.deleteDrainRequest())
        )
    );

    deleteDrainRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.deleteDrainRequest),
            concatLatestFrom(() => [
                this.store.select(selectDrainProcessGroupId).pipe(isDefinedAndNotNull()),
                this.store.select(selectDrainRequestId).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, processGroupId, drainRequestId]) =>
                this.processGroupDrainService.deleteDrainRequest(processGroupId, drainRequestId).pipe(
                    map((response) =>
                        ProcessGroupDrainActions.deleteDrainRequestSuccess({
                            response: { drainRequestEntity: response }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ProcessGroupDrainActions.processGroupDrainApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    ),
                    takeUntil(this.actions$.pipe(ofType(ProcessGroupDrainActions.resetProcessGroupDrainState)))
                )
            )
        )
    );

    drainEnded$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.deleteDrainRequestSuccess, ProcessGroupDrainActions.drainRequestEnded),
            map(() => FlowActions.reloadFlow())
        )
    );

    processGroupDrainApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ProcessGroupDrainActions.processGroupDrainApiError),
            map((action) => action.error),
            tap(() => {
                this.drainDialogRef?.close();
            }),
            map((error) => ErrorActions.snackBarError({ error }))
        )
    );

    fullScreenError$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ErrorActions.fullScreenError),
                tap(() => {
                    this.drainDialogRef?.close();
                })
            ),
        { dispatch: false }
    );
}
