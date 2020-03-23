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

import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalTransitionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.execution.statemachine.RetryableTransitionException;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.extern.slf4j.Slf4j;

/**
 * Wait for job process to exit.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class WaitJobCompletionStage extends ExecutionStage {
    private final JobProcessManager jobProcessManager;

    /**
     * Constructor.
     *
     * @param jobProcessManager the job process manager
     */
    public WaitJobCompletionStage(final JobProcessManager jobProcessManager) {
        super(States.WAIT_JOB_COMPLETION);
        this.jobProcessManager = jobProcessManager;
    }

    @Override
    protected void attemptTransition(
        final ExecutionContext executionContext
    ) throws RetryableTransitionException, FatalTransitionException {

        // In case of abort, this state may be reached even if there was no attempt to launch the process.
        if (executionContext.isJobLaunched()) {
            log.info("Monitoring job process");
            final JobProcessResult jobProcessResult;
            try {
                jobProcessResult = this.jobProcessManager.waitFor();
            } catch (final InterruptedException e) {
                throw createFatalException(e);
            }

            executionContext.setJobProcessResult(jobProcessResult);

            final JobStatus finalJobStatus = jobProcessResult.getFinalStatus();
            UserConsole.getLogger().info("Job process completed with final status {}", finalJobStatus);
        } else {
            log.debug("Job not launched, skipping");
        }

    }
}
