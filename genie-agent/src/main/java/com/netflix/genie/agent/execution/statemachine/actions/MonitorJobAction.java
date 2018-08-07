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
 *
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

        try {
            // Evil-but-necessary little hack.
            // The agent and the child job process receive SIGINT at the same time.
            // If the child terminates quickly, the code below will execute before the signal handler has a chance to
            // set the job as killed, and the final status would be (incorrectly) reported as failed (due to non-zero
            // exit code).
            // So give the handler a chance to mark the context before attempting to read it.
            Thread.sleep(100);
        } catch (final InterruptedException e) {
            // Do nothing.
        }

        final ExecutionContext.KillSource killSource = executionContext.getJobKillSource();

        log.info("Job process completed with exit code: {} (kill source: )", exitCode, killSource);

        final JobStatus finalJobStatus;
        if (killSource != null) {
            finalJobStatus = JobStatus.KILLED;
        } else if (exitCode == 0) {
            finalJobStatus = JobStatus.SUCCEEDED;
        } else {
            finalJobStatus = JobStatus.FAILED;
        }

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
