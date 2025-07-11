/*
 *
 *  Copyright 2020 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.agent.execution.statemachine.stages;

import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;

import jakarta.validation.constraints.NotBlank;

/**
 * Starts the kill service.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class StartKillServiceStage extends StartServiceStage {
    private final AgentJobKillService killService;

    /**
     * Constructor.
     *
     * @param killService kill service
     */
    public StartKillServiceStage(final AgentJobKillService killService) {
        super(States.START_KILL_SERVICE);
        this.killService = killService;
    }

    @Override
    protected void startService(final @NotBlank String claimedJobId, final ExecutionContext executionContext) {
        this.killService.start(claimedJobId);
    }
}
