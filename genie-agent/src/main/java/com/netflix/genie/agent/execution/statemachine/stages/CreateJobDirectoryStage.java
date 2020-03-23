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
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalTransitionException;
import com.netflix.genie.agent.execution.statemachine.RetryableTransitionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * Creates the job directory.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class CreateJobDirectoryStage extends ExecutionStage {
    private final JobSetupService jobSetupService;

    /**
     * Constructor.
     *
     * @param jobSetupService job setup service
     */
    public CreateJobDirectoryStage(final JobSetupService jobSetupService) {
        super(States.CREATE_JOB_DIRECTORY);
        this.jobSetupService = jobSetupService;
    }

    @Override
    protected void attemptTransition(
        final ExecutionContext executionContext
    ) throws RetryableTransitionException, FatalTransitionException {

        final JobSpecification jobSpecification = executionContext.getJobSpecification();
        assert jobSpecification != null;

        log.info("Creating job directory");

        final File jobDirectory;
        try {
            jobDirectory = this.jobSetupService.createJobDirectory(jobSpecification);
        } catch (SetUpJobException e) {
            throw createFatalException(e);
        }

        executionContext.setJobDirectory(jobDirectory);

        UserConsole.getLogger().info("Created job directory: {}", jobDirectory);
    }
}
