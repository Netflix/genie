/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Implementation of {@link KillService}.
 *
 * @author standon
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
class KillServiceImpl implements KillService {

    private final JobExecutionStateMachine jobExecutionStateMachine;
    private final LaunchJobService launchJobService;


    KillServiceImpl(
        final JobExecutionStateMachine jobExecutionStateMachine,
        final LaunchJobService launchJobService
    ) {
        this.jobExecutionStateMachine = jobExecutionStateMachine;
        this.launchJobService = launchJobService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill(final KillSource killSource) {

        // Cut job execution state machine short.
        jobExecutionStateMachine.stop();

        // If the kill was a system signal, the job process already received it too.
        // (by virtue of being in the same process group as the agent)
        // Avoid sending a second SIGINT
        final boolean sendSigIntToJobProcess = killSource != KillSource.SYSTEM_SIGNAL;

        // Make sure the process doesn't get started.
        // If it was started already, kill it and mark it as such.
        launchJobService.kill(sendSigIntToJobProcess);
    }
}
