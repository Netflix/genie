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
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.dto.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Action performed when in state MONITOR_JOB.
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class MonitorJobAction extends BaseStateAction implements StateAction.MonitorJob {

    private final AgentJobService agentJobService;

    MonitorJobAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService
    ) {
        super(executionContext);
        this.agentJobService = agentJobService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        log.info("Monitoring job...");

        final int exitCode;
        try {
             exitCode = executionContext.getJobProcess().waitFor();
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for job completion", e);
        }

        log.info("Job process completed with exit code: {}", exitCode);

        // TODO: handle KILLED case
        final JobStatus finalJobStatus = exitCode == 0 ? JobStatus.SUCCEEDED : JobStatus.FAILED;

        executionContext.setFinalJobStatus(finalJobStatus);

        try {
            this.agentJobService.changeJobStatus(
                executionContext.getClaimedJobId(),
                executionContext.getCurrentJobStatus(),
                finalJobStatus,
                "Job process exited with status " + exitCode
            );
            executionContext.setCurrentJobStatus(finalJobStatus);
        } catch (ChangeJobStatusException e) {
            throw new RuntimeException("Failed to update job status", e);
        }

        return Events.MONITOR_JOB_COMPLETE;
    }
}
