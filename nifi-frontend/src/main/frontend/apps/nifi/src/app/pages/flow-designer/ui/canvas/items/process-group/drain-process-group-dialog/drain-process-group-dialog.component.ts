/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, EventEmitter, inject, Input, Output } from '@angular/core';
import { AsyncPipe } from '@angular/common';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { MatProgressBar } from '@angular/material/progress-bar';
import { Observable, of } from 'rxjs';
import { ErrorBanner, NiFiCommon, NifiTooltipDirective } from '@nifi/shared';
import {
    DrainBlockingComponent,
    isDrained,
    ProcessGroupDrainRequest,
    ProcessGroupDrainState
} from '../../../../../state/process-group-drain';
import { ValidationErrorsTipInput } from '../../../../../../../state/shared';
import { ValidationErrorsTip } from '../../../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';

@Component({
    selector: 'drain-process-group-dialog',
    imports: [MatDialogModule, MatButton, AsyncPipe, MatProgressBar, ErrorBanner, NifiTooltipDirective],
    templateUrl: './drain-process-group-dialog.component.html',
    styleUrl: './drain-process-group-dialog.component.scss'
})
export class DrainProcessGroupDialog {
    private nifiCommon = inject(NiFiCommon);

    @Input() drainState$: Observable<ProcessGroupDrainState | null> = of(null);
    @Output() cancelDrain: EventEmitter<void> = new EventEmitter<void>();
    @Output() goToComponent: EventEmitter<DrainBlockingComponent> = new EventEmitter<DrainBlockingComponent>();

    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    getStatusMessage(drainState: ProcessGroupDrainState): string {
        if (drainState.status === 'ended') {
            if (!drainState.drainRequest) {
                return 'The drain is no longer available, so the number of FlowFiles that remain queued is unknown.';
            }
            if (drainState.drainRequest.state === 'FAILED') {
                return 'The drain failed.';
            }
            const remainingCount = this.nifiCommon.formatInteger(drainState.drainRequest.currentCount);
            // a source that had not yet stopped when the drain ended may still bring data into the Process Group
            if (!drainState.drainRequest.prepared) {
                return `Drain ended before the source components stopped. ${remainingCount} FlowFiles remain queued.`;
            }
            if (isDrained(drainState.drainRequest)) {
                return 'Drain complete. Source components remain stopped.';
            }
            return `Drain ended. ${remainingCount} FlowFiles remain queued.`;
        }
        if (drainState.status === 'ending') {
            return 'Ending drain...';
        }
        if (drainState.drainRequest && !drainState.drainRequest.prepared) {
            return 'Waiting for the source components to stop...';
        }
        if (drainState.drainRequest && isDrained(drainState.drainRequest)) {
            return 'No FlowFiles remain. Finishing drain...';
        }
        if (drainState.drainRequest) {
            return 'Waiting for the queued data to flow out of the Process Group...';
        }
        return 'Loading drain status...';
    }

    getPercentCompleted(drainRequest: ProcessGroupDrainRequest): number {
        if (drainRequest.originalCount === 0) {
            return 100;
        }
        const percentCompleted = Math.round(
            ((drainRequest.originalCount - drainRequest.currentCount) * 100) / drainRequest.originalCount
        );
        return Math.min(Math.max(percentCompleted, 0), 100);
    }

    getRemainingText(drainRequest: ProcessGroupDrainRequest): string {
        const currentCount = this.nifiCommon.formatInteger(drainRequest.currentCount);
        const currentSize = this.nifiCommon.formatDataSize(drainRequest.currentSize);
        const originalCount = this.nifiCommon.formatInteger(drainRequest.originalCount);
        return `${currentCount} FlowFiles (${currentSize}) remaining of ${originalCount}`;
    }

    getBlockingComponentMessage(component: DrainBlockingComponent): string {
        const name = component.name ?? component.id;
        const reason = component.reason === 'DISABLED' ? 'disabled' : 'invalid';
        const queuedCount = this.nifiCommon.formatInteger(component.queuedCount);
        return `'${name}' is ${reason} and has ${queuedCount} FlowFiles queued`;
    }

    getValidationErrorsTipData(component: DrainBlockingComponent): ValidationErrorsTipInput {
        return {
            isValidating: false,
            validationErrors: component.validationErrors ?? []
        };
    }
}
