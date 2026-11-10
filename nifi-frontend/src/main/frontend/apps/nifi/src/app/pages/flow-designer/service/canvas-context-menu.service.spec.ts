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

import { TestBed } from '@angular/core/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import type { Mock } from 'vitest';

import { CanvasContextMenu } from './canvas-context-menu.service';
import { CanvasUtils } from './canvas-utils.service';
import { Client } from '../../../service/client.service';
import { CanvasView } from './canvas-view.service';
import { CanvasActionsService } from './canvas-actions.service';
import { DraggableBehavior } from './behavior/draggable-behavior.service';
import * as FlowActions from '../state/flow/flow.actions';
import * as ProcessGroupDrainActions from '../state/process-group-drain/process-group-drain.actions';
import type { ResolvedExecutionEngine } from '../state/flow';
import type { ContextMenuItemDefinition } from '../../../ui/common/context-menu/context-menu.component';

interface SetupOptions {
    currentProcessGroupId?: string;
    isProcessGroup?: boolean;
    resolvedExecutionEngine?: ResolvedExecutionEngine;
}

function menuItem(menuItems: ContextMenuItemDefinition[], text: string): ContextMenuItemDefinition {
    const item = menuItems.find((candidate) => candidate.text === text);
    if (!item) {
        throw new Error(`Expected menu item "${text}"`);
    }
    return item;
}

async function setup(options: SetupOptions = {}) {
    const currentProcessGroupId = options.currentProcessGroupId ?? 'current-pg';
    const canvasUtils = {
        getProcessGroupId: vi.fn().mockReturnValue(currentProcessGroupId),
        isProcessGroup: vi.fn().mockReturnValue(options.isProcessGroup ?? false),
        getResolvedExecutionEngine: vi.fn().mockReturnValue(options.resolvedExecutionEngine ?? 'STANDARD'),
        canModifyCurrentGroup: vi.fn().mockReturnValue(true),
        getCurrentProcessGroupDrainRequestId: vi.fn().mockReturnValue(null)
    };

    await TestBed.configureTestingModule({
        providers: [
            CanvasContextMenu,
            provideMockStore(),
            { provide: CanvasUtils, useValue: canvasUtils },
            { provide: Client, useValue: {} },
            { provide: CanvasView, useValue: {} },
            {
                provide: CanvasActionsService,
                useValue: {
                    getConditionFunction: () => () => false,
                    getActionFunction: () => () => undefined
                }
            },
            { provide: DraggableBehavior, useValue: {} }
        ]
    }).compileComponents();

    const service = TestBed.inject(CanvasContextMenu);
    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch') as Mock;

    return { service, canvasUtils, dispatchSpy };
}

describe('CanvasContextMenu', () => {
    describe('Stop Sources', () => {
        it('is immediately after Stop in the root menu', async () => {
            const { service } = await setup();
            const menuItems = service.getMenu('root')!.menuItems;
            const texts = menuItems.map((item) => item.text);
            const stopIndex = texts.indexOf('Stop');
            const stopSourcesIndex = texts.indexOf('Stop Sources');

            expect(stopIndex).toBeGreaterThan(-1);
            expect(stopSourcesIndex).toBe(stopIndex + 1);
        });

        it('is visible on an empty canvas and dispatches stopSources for the current group', async () => {
            const { service, canvasUtils, dispatchSpy } = await setup({ currentProcessGroupId: 'current-pg' });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = { empty: () => true };

            expect(stopSources.condition!(selection as never)).toBe(true);
            stopSources.action!(selection as never);

            expect(canvasUtils.getProcessGroupId).toHaveBeenCalled();
            expect(dispatchSpy).toHaveBeenCalledWith(FlowActions.stopSources({ request: { id: 'current-pg' } }));
        });

        it('is visible for a selected process group and dispatches stopSources for that group', async () => {
            const { service, dispatchSpy } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STANDARD' })
            };

            expect(stopSources.condition!(selection as never)).toBe(true);
            stopSources.action!(selection as never);

            expect(dispatchSpy).toHaveBeenCalledWith(FlowActions.stopSources({ request: { id: 'pg-child' } }));
        });

        it('is hidden when the selection is not a process group', async () => {
            const { service } = await setup({ isProcessGroup: false });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = { empty: () => false };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });

        it('is hidden on an empty canvas when the current group resolves to STATELESS', async () => {
            const { service } = await setup({ resolvedExecutionEngine: 'STATELESS' });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = { empty: () => true };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });

        it('is hidden for a selected process group when the resolved engine is missing', async () => {
            const { service } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child' })
            };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });

        it('is visible on an empty canvas when the current group is configured INHERITED but resolves to STANDARD', async () => {
            const { service } = await setup({ resolvedExecutionEngine: 'STANDARD' });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = { empty: () => true };

            expect(stopSources.condition!(selection as never)).toBe(true);
        });

        it('is visible for a selected process group with no component when the entity resolves to STANDARD', async () => {
            const { service } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STANDARD' })
            };

            expect(stopSources.condition!(selection as never)).toBe(true);
        });

        it('is hidden for a selected process group with no component when the entity resolves to STATELESS', async () => {
            const { service } = await setup({ isProcessGroup: true });
            const stopSources = menuItem(service.getMenu('root')!.menuItems, 'Stop Sources');
            const selection = {
                empty: () => false,
                datum: () => ({ id: 'pg-child', resolvedExecutionEngine: 'STATELESS' })
            };

            expect(stopSources.condition!(selection as never)).toBe(false);
        });
    });

    describe('Drain and View Drain', () => {
        function processGroupSelection(overrides: Record<string, unknown> = {}) {
            const selectionData = {
                id: 'pg-child',
                resolvedExecutionEngine: 'STANDARD',
                permissions: { canRead: true, canWrite: true },
                component: { id: 'pg-child' },
                ...overrides
            };
            return { empty: () => false, datum: () => selectionData };
        }

        it('offers Drain only for a writable STANDARD process group without a drain', async () => {
            const { service, canvasUtils, dispatchSpy } = await setup({ isProcessGroup: true });
            const drain = menuItem(service.getMenu('root')!.menuItems, 'Drain');

            expect(drain.condition!(processGroupSelection() as never)).toBe(true);
            expect(drain.condition!(processGroupSelection({ resolvedExecutionEngine: 'STATELESS' }) as never)).toBe(
                false
            );
            expect(
                drain.condition!(processGroupSelection({ permissions: { canRead: true, canWrite: false } }) as never)
            ).toBe(false);
            expect(
                drain.condition!(
                    processGroupSelection({ component: { id: 'pg-child', drainRequestId: 'drain-1' } }) as never
                )
            ).toBe(false);

            drain.action!(processGroupSelection() as never);
            expect(dispatchSpy).toHaveBeenCalledWith(
                ProcessGroupDrainActions.promptDrainRequest({ request: { processGroupId: 'pg-child' } })
            );

            // an empty selection refers to the current group
            const emptySelection = { empty: () => true };
            expect(drain.condition!(emptySelection as never)).toBe(true);
            drain.action!(emptySelection as never);
            expect(dispatchSpy).toHaveBeenCalledWith(
                ProcessGroupDrainActions.promptDrainRequest({ request: { processGroupId: 'current-pg' } })
            );

            canvasUtils.getCurrentProcessGroupDrainRequestId.mockReturnValue('drain-1');
            expect(drain.condition!(emptySelection as never)).toBe(false);

            canvasUtils.getCurrentProcessGroupDrainRequestId.mockReturnValue(null);
            canvasUtils.canModifyCurrentGroup.mockReturnValue(false);
            expect(drain.condition!(emptySelection as never)).toBe(false);

            canvasUtils.canModifyCurrentGroup.mockReturnValue(true);
            canvasUtils.getResolvedExecutionEngine.mockReturnValue('STATELESS');
            expect(drain.condition!(emptySelection as never)).toBe(false);
        });

        it('offers View Drain only for a writable process group with a drain and polls that drain', async () => {
            const { service, canvasUtils, dispatchSpy } = await setup({ isProcessGroup: true });
            const viewDrain = menuItem(service.getMenu('root')!.menuItems, 'View Drain');
            const draining = processGroupSelection({ component: { id: 'pg-child', drainRequestId: 'drain-1' } });

            expect(viewDrain.condition!(draining as never)).toBe(true);
            expect(viewDrain.condition!(processGroupSelection() as never)).toBe(false);
            expect(
                viewDrain.condition!(
                    processGroupSelection({
                        permissions: { canRead: true, canWrite: false },
                        component: { id: 'pg-child', drainRequestId: 'drain-1' }
                    }) as never
                )
            ).toBe(false);

            viewDrain.action!(draining as never);
            expect(dispatchSpy).toHaveBeenCalledWith(
                ProcessGroupDrainActions.viewDrainRequest({
                    request: { processGroupId: 'pg-child', drainRequestId: 'drain-1' }
                })
            );

            // an empty selection refers to the current group
            const emptySelection = { empty: () => true };
            expect(viewDrain.condition!(emptySelection as never)).toBe(false);

            canvasUtils.getCurrentProcessGroupDrainRequestId.mockReturnValue('drain-current');
            expect(viewDrain.condition!(emptySelection as never)).toBe(true);
            viewDrain.action!(emptySelection as never);
            expect(dispatchSpy).toHaveBeenCalledWith(
                ProcessGroupDrainActions.viewDrainRequest({
                    request: { processGroupId: 'current-pg', drainRequestId: 'drain-current' }
                })
            );

            canvasUtils.canModifyCurrentGroup.mockReturnValue(false);
            expect(viewDrain.condition!(emptySelection as never)).toBe(false);
        });

        it('are hidden when the selection is not a single process group', async () => {
            const { service } = await setup({ isProcessGroup: false });
            const menuItems = service.getMenu('root')!.menuItems;
            const selection = processGroupSelection({ component: { id: 'pg-child', drainRequestId: 'drain-1' } });

            expect(menuItem(menuItems, 'Drain').condition!(selection as never)).toBe(false);
            expect(menuItem(menuItems, 'View Drain').condition!(selection as never)).toBe(false);
        });
    });
});
