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
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;

/**
 * Stops the job kill service.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class StopKillServiceStage extends StopServiceStage {
    private AgentJobKillService killService;

    /**
     * Constructor.
     *
     * @param killService kill service
     */
    public StopKillServiceStage(final AgentJobKillService killService) {
        super(States.STOP_KILL_SERVICE);
        this.killService = killService;
    }

    @Override
    protected void stopService() {
        killService.stop();
    }
}
