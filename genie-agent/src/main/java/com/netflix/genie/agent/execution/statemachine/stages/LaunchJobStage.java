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
import com.netflix.genie.agent.execution.exceptions.JobLaunchException;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * Launches the job process.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class LaunchJobStage extends ExecutionStage {
    private final JobProcessManager jobProcessManager;

    /**
     * Constructor.
     *
     * @param jobProcessManager job process manager
     */
    public LaunchJobStage(final JobProcessManager jobProcessManager) {
        super(States.LAUNCH_JOB);
        this.jobProcessManager = jobProcessManager;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final File jobDirectory = executionContext.getJobDirectory();
        final File jobScript = executionContext.getJobScript();
        final boolean runFromJobDirectory = executionContext.isRunFromJobDirectory();
        final JobSpecification jobSpecification = executionContext.getJobSpecification();

        assert jobDirectory != null;
        assert jobScript != null;
        assert jobSpecification != null;

        final Integer timeout = jobSpecification.getTimeout().orElse(null);
        final boolean interactive = jobSpecification.isInteractive();

        log.info("Launching job");
        try {
            this.jobProcessManager.launchProcess(
                jobDirectory,
                jobScript,
                interactive,
                timeout,
                runFromJobDirectory
            );
        } catch (final JobLaunchException e) {
            throw createFatalException(e);
        }

        executionContext.setJobLaunched(true);
        executionContext.setNextJobStatus(JobStatus.RUNNING);
        executionContext.setNextJobStatusMessage(JobStatusMessages.JOB_RUNNING);

        ConsoleLog.getLogger().info("Job launched");
    }
}
