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

import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.services.JobMonitorService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
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
    private final JobMonitorService jobMonitorService;

    /**
     * Constructor.
     *
     * @param jobProcessManager the job process manager
     * @param jobMonitorService the job monitor service
     */
    public WaitJobCompletionStage(
        final JobProcessManager jobProcessManager,
        final JobMonitorService jobMonitorService
    ) {
        super(States.WAIT_JOB_COMPLETION);
        this.jobProcessManager = jobProcessManager;
        this.jobMonitorService = jobMonitorService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        // In case of abort, this state may be reached even if there was no attempt to launch the process.
        if (executionContext.isJobLaunched()) {
            log.info("Monitoring job process");
            this.jobMonitorService.start(
                executionContext.getClaimedJobId(),
                executionContext.getJobDirectory().toPath()
            );
            final JobProcessResult jobProcessResult;
            try {
                jobProcessResult = this.jobProcessManager.waitFor();
            } catch (final InterruptedException e) {
                throw createFatalException(e);
            } finally {
                this.jobMonitorService.stop();
            }

            executionContext.setJobProcessResult(jobProcessResult);

            final JobStatus finalJobStatus = jobProcessResult.getFinalStatus();
            ConsoleLog.getLogger().info("Job process completed with final status {}", finalJobStatus);
        } else {
            log.debug("Job not launched, skipping");
        }
    }
}
