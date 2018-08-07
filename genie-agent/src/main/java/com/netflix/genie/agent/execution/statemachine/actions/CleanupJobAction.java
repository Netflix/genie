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

import java.util.List;

/**
 * Action performed when in state CLEANUP_JOB.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class CleanupJobAction extends BaseStateAction implements StateAction.CleanupJob {

    private final AgentJobService agentJobService;

    CleanupJobAction(
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
        log.info("Cleaning up job...");

        // Set if this job got past the state where a job ID is successfully claimed.
        final String claimedJobId = executionContext.getClaimedJobId();

        // Set if the job was launched, null if that state was never reached,
        // for example due to CANCEL_JOB_LAUNCH event.
        final JobStatus finalJobStatus = executionContext.getFinalJobStatus();

        // Reason for the job being killed.
        final ExecutionContext.KillSource killSource = executionContext.getJobKillSource();

        // Last job status the server was made aware of.
        final JobStatus lastJobStatus = executionContext.getCurrentJobStatus();

        if (lastJobStatus != null && claimedJobId != null) {
            // A job was claimed, but final status is not set (due to error/kill/...).
            // server should be made aware of final state.
            if (finalJobStatus == null) {
                // This job was killed
                if (killSource != null) {
                    try {
                        // Job launch was aborted before the the job even started.
                        agentJobService.changeJobStatus(
                            claimedJobId,
                            lastJobStatus,
                            JobStatus.KILLED,
                            "Terminated by user via " + killSource.name()
                        );
                    } catch (final ChangeJobStatusException e) {
                        throw new RuntimeException("Failed to update server status", e);
                    }
                    executionContext.setFinalJobStatus(JobStatus.KILLED);
                } else {
                    // No job final status (which reflect server-side status), and no sign of abort/kill.
                    throw new IllegalStateException(
                        "Reached cleanup state and finalJobState is null. Last job state: " + lastJobStatus
                    );
                }
            } else {
                log.debug("Job final status already updated server-side: {}", finalJobStatus);
            }
        } else {
            log.debug("Job never claimed an ID, skipping server-side status update");
        }

        final List<StateAction> cleanupActions = executionContext.getCleanupActions();
        for (final StateAction cleanupAction : cleanupActions) {

            if (cleanupAction == this) {
                // Skip self
                continue;
            }

            try {
                cleanupAction.cleanup();
            } catch (final Exception e) {
                log.warn(
                    "Exception during action {} cleanup",
                    cleanupAction.getClass().getSimpleName(),
                    e
                );
            }
        }

        return Events.CLEANUP_JOB_COMPLETE;
    }
}
