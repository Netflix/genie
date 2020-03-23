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

import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.statemachine.States;

/**
 * Stops the file streaming service.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class StopFileServiceStage extends StopServiceStage {
    private AgentFileStreamService agentFileStreamService;

    /**
     * Constructor.
     *
     * @param agentFileStreamService agent file stream service
     */
    public StopFileServiceStage(final AgentFileStreamService agentFileStreamService) {
        super(States.STOP_FILES_STREAM_SERVICE);
        this.agentFileStreamService = agentFileStreamService;
    }

    @Override
    protected void stopService() {
        this.agentFileStreamService.stop();
    }
}
