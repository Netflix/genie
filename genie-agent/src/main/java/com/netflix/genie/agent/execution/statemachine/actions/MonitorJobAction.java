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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.dto.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Action performed when in state MONITOR_JOB.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class MonitorJobAction extends BaseStateAction implements StateAction.MonitorJob {

    private final AgentJobService agentJobService;
    private final LaunchJobService launchJobService;

    MonitorJobAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService,
        final LaunchJobService launchJobService
    ) {
        super(executionContext);
        this.agentJobService = agentJobService;
        this.launchJobService = launchJobService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        log.info("Monitoring job...");

        final JobStatus finalJobStatus;
        try {
            finalJobStatus = launchJobService.waitFor();
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for job process completion", e);
        }

        log.info("Job process completed with final status {}", finalJobStatus);

        try {
            this.agentJobService.changeJobStatus(
                executionContext.getClaimedJobId(),
                executionContext.getCurrentJobStatus(),
                finalJobStatus,
                "Job process completed with final status " + finalJobStatus
            );
            executionContext.setCurrentJobStatus(finalJobStatus);
            executionContext.setFinalJobStatus(finalJobStatus);
        } catch (ChangeJobStatusException e) {
            throw new RuntimeException("Failed to update job status", e);
        }

        return Events.MONITOR_JOB_COMPLETE;
    }
}
