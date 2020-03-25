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

import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.States;

/**
 * Updates job status when job starts initializing.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class SetJobStatusInit extends UpdateJobStatusStage {

    /**
     * Constructor.
     *
     * @param agentJobService the agent job service
     */
    public SetJobStatusInit(
        final AgentJobService agentJobService
    ) {
        super(agentJobService, States.SET_STATUS_INIT);
    }
}
