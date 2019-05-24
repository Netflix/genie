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

import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.dto.JobStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Action performed when in state HANDLE_ERROR.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class HandleErrorAction extends BaseStateAction implements StateAction.HandleError {

    private final AgentJobService agentJobService;

    HandleErrorAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService
    ) {
        super(executionContext);
        this.agentJobService = agentJobService;
    }

    @Override
    protected void executePreActionValidation() {
        assertCurrentJobStatusPresentIfJobIdPresent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        UserConsole.getLogger().info("Handling execution error...");

        final Optional<JobStatus> finalJobStatus = executionContext.getFinalJobStatus();
        final Optional<String> claimedJobId = executionContext.getClaimedJobId();

        if (claimedJobId.isPresent() && !finalJobStatus.isPresent()) {
            try {
                agentJobService.changeJobStatus(
                    claimedJobId.get(),
                    executionContext.getCurrentJobStatus().get(),
                    JobStatus.FAILED,
                    "Setting failed status due to execution error"
                );
                executionContext.setCurrentJobStatus(JobStatus.FAILED);
                executionContext.setFinalJobStatus(JobStatus.FAILED);
            } catch (ChangeJobStatusException e) {
                log.error("Failed to update job status as part of execution error handling");
            } catch (final Exception e) {
                log.error("Unrecoverable error during error handling.", e);
                UserConsole.getLogger().error("Failed remediation step. Shutting down.");
            }
        }

        return Events.HANDLE_ERROR_COMPLETE;
    }

    @Override
    protected void executePostActionValidation() {
    }
}
