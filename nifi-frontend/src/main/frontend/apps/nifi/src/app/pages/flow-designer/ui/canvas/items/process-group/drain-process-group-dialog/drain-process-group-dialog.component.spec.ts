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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { By } from '@angular/platform-browser';
import { MatProgressBar } from '@angular/material/progress-bar';
import { BehaviorSubject } from 'rxjs';
import { NifiTooltipDirective } from '@nifi/shared';
import { DrainProcessGroupDialog } from './drain-process-group-dialog.component';
import {
    DrainBlockingComponent,
    ProcessGroupDrainRequest,
    ProcessGroupDrainState,
    ProcessGroupDrainStatus
} from '../../../../../state/process-group-drain';
import { createDrainRequest } from '../../../../../testing/process-group-drain-fixture';

const INVALID_PROCESSOR: DrainBlockingComponent = {
    id: 'processor-1',
    groupId: 'pg-1',
    type: 'PROCESSOR',
    name: 'Validate File',
    reason: 'INVALID',
    validationErrors: ["'Filename' is required"],
    queuedCount: 3
};

const DISABLED_PORT: DrainBlockingComponent = {
    id: 'port-1',
    groupId: 'pg-child',
    type: 'OUTPUT_PORT',
    reason: 'DISABLED',
    queuedCount: 1
};

function createDrainState(
    status: ProcessGroupDrainStatus,
    drainRequestOverrides: Partial<ProcessGroupDrainRequest> = {}
): ProcessGroupDrainState {
    return {
        processGroupId: 'pg-1',
        drainRequestId: 'drain-1',
        drainRequest: createDrainRequest({
            blockingComponents: [INVALID_PROCESSOR, DISABLED_PORT],
            ...drainRequestOverrides
        }),
        status
    };
}

describe('DrainProcessGroupDialog', () => {
    let component: DrainProcessGroupDialog;
    let fixture: ComponentFixture<DrainProcessGroupDialog>;
    let drainState$: BehaviorSubject<ProcessGroupDrainState>;

    function textOf(selector: string): string {
        return fixture.debugElement.query(By.css(selector)).nativeElement.textContent.trim();
    }

    function queryAll(selector: string) {
        return fixture.debugElement.queryAll(By.css(selector));
    }

    function progressValue(): number {
        return (fixture.debugElement.query(By.directive(MatProgressBar)).componentInstance as MatProgressBar).value;
    }

    function showState(drainState: ProcessGroupDrainState): void {
        drainState$.next(drainState);
        fixture.detectChanges();
    }

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [DrainProcessGroupDialog, NoopAnimationsModule],
            providers: [{ provide: MatDialogRef, useValue: { close: vi.fn() } }]
        }).compileComponents();

        fixture = TestBed.createComponent(DrainProcessGroupDialog);
        component = fixture.componentInstance;
        drainState$ = new BehaviorSubject<ProcessGroupDrainState>(createDrainState('draining'));
        component.drainState$ = drainState$;
        fixture.detectChanges();
    });

    it('should show progress, warnings, and Cancel Drain but no Go To links while draining', () => {
        const cancelSpy = vi.spyOn(component.cancelDrain, 'next');

        const progressBar = fixture.debugElement.query(By.directive(MatProgressBar))
            .componentInstance as MatProgressBar;
        expect(progressBar.mode).toBe('determinate');
        expect(progressValue()).toBe(60);
        expect(textOf('[data-qa="drain-remaining"]')).toBe('4 FlowFiles (400.00 bytes) remaining of 10');

        const messages = queryAll('[data-qa="drain-blocking-component-message"]').map((element) =>
            element.nativeElement.textContent.trim()
        );
        expect(messages).toEqual([
            "'Validate File' is invalid and has 3 FlowFiles queued",
            "'port-1' is disabled and has 1 FlowFiles queued"
        ]);
        // hovering over the warning shows the validation errors, so a component without any has no tooltip
        const warningTooltips = queryAll('[data-qa="drain-blocking-component-warning"]').map(
            (element) => element.injector.get(NifiTooltipDirective).tooltipDisabled
        );
        expect(warningTooltips).toEqual([false, true]);
        expect(queryAll('[data-qa="drain-go-to"]')).toHaveLength(0);
        expect(queryAll('[data-qa="drain-close"]')).toHaveLength(0);

        fixture.debugElement.query(By.css('[data-qa="drain-cancel"]')).nativeElement.click();
        expect(cancelSpy).toHaveBeenCalled();

        // NiFi ends the drain shortly after the last FlowFile leaves, so the drain is still active with none remaining
        showState(createDrainState('draining', { currentCount: 0 }));
        expect(textOf('[data-qa="drain-status-message"]')).toBe('No FlowFiles remain. Finishing drain...');
        expect(fixture.debugElement.query(By.css('[data-qa="drain-cancel"]')).nativeElement.disabled).toBe(false);

        // a source that is still running may bring more data into the Process Group, even when no FlowFiles are queued
        showState(createDrainState('draining', { currentCount: 0, prepared: false }));
        expect(textOf('[data-qa="drain-status-message"]')).toBe('Waiting for the source components to stop...');

        showState(createDrainState('ending', { originalCount: 3, currentCount: 1 }));
        expect(textOf('[data-qa="drain-status-message"]')).toBe('Ending drain...');
        expect(fixture.debugElement.query(By.css('[data-qa="drain-cancel"]')).nativeElement.disabled).toBe(true);
        expect(progressValue()).toBe(67);
    });

    it('should show the completed and ended messages, Close, and Go To links once the drain has ended', () => {
        const goToSpy = vi.spyOn(component.goToComponent, 'next');

        showState(createDrainState('ended', { state: 'ENDED', originalCount: 0, currentCount: 0 }));
        expect(textOf('[data-qa="drain-status-message"]')).toBe('Drain complete. Source components remain stopped.');
        expect(progressValue()).toBe(100);
        expect(queryAll('[data-qa="drain-cancel"]')).toHaveLength(0);
        expect(queryAll('[data-qa="drain-close"]')).toHaveLength(1);
        const links = queryAll('[data-qa="drain-go-to"]');
        expect(links).toHaveLength(2);
        links[1].nativeElement.click();
        expect(goToSpy).toHaveBeenCalledWith(DISABLED_PORT);

        // more FlowFiles can be queued than when the drain started, which shows as no progress
        showState(createDrainState('ended', { state: 'ENDED', currentCount: 12 }));
        expect(textOf('[data-qa="drain-status-message"]')).toBe('Drain ended. 12 FlowFiles remain queued.');
        expect(progressValue()).toBe(0);

        // a drain that is cancelled before the sources have stopped is not complete, because a source may still bring in data
        showState(createDrainState('ended', { state: 'ENDED', currentCount: 0, prepared: false }));
        expect(textOf('[data-qa="drain-status-message"]')).toBe(
            'Drain ended before the source components stopped. 0 FlowFiles remain queued.'
        );

        // a drain that no longer exists, for example because NiFi restarted, has no final status
        showState({ ...createDrainState('ended'), drainRequest: null });
        expect(textOf('[data-qa="drain-status-message"]')).toBe(
            'The drain is no longer available, so the number of FlowFiles that remain queued is unknown.'
        );
        expect(queryAll('[data-qa="drain-remaining"]')).toHaveLength(0);
        expect(queryAll('[data-qa="drain-close"]')).toHaveLength(1);
    });

    it('should show the failure reason when the drain failed', () => {
        showState(createDrainState('ended', { state: 'FAILED', failureReason: 'Failed to enable Controller Service' }));

        expect(fixture.nativeElement.querySelector('error-banner').textContent).toContain(
            'Failed to enable Controller Service'
        );
        expect(textOf('[data-qa="drain-status-message"]')).toBe('The drain failed.');
        expect(queryAll('[data-qa="drain-close"]')).toHaveLength(1);
    });
});
