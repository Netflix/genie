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

        // If execution was aborted sometimes before the job was launched, the server is due for a job status update.
        final String claimedJobId = executionContext.getClaimedJobId();
        final JobStatus finalJobStatus = executionContext.getFinalJobStatus();
        if (claimedJobId != null && finalJobStatus == null) {
            // This job is tracked server-side (an ID was claimed), but the server was not updated with a final
            // status. The only path that leads to this state is a CANCEL_JOB_LAUNCH transition.
            try {
                agentJobService.changeJobStatus(
                    claimedJobId,
                    executionContext.getCurrentJobStatus(),
                    JobStatus.KILLED,
                    "Job aborted before process launch"
                );
                executionContext.setCurrentJobStatus(JobStatus.KILLED);
                executionContext.setFinalJobStatus(JobStatus.KILLED);
            } catch (final ChangeJobStatusException e) {
                throw new RuntimeException("Failed to update server status", e);
            }
        }

        // For each state action performed, perform the corresponding cleanup.
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
