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

import com.netflix.genie.agent.execution.CleanupStrategy;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * Performs cleanup of the job directory after execution.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class CleanupJobDirectoryStage extends ExecutionStage {
    private JobSetupService jobSetupService;

    /**
     * Constructor.
     *
     * @param jobSetupService job setup service
     */
    public CleanupJobDirectoryStage(final JobSetupService jobSetupService) {
        super(States.CLEAN);
        this.jobSetupService = jobSetupService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {
        final File jobDirectory = executionContext.getJobDirectory();
        final CleanupStrategy cleanupStrategy = executionContext.getCleanupStrategy();

        if (jobDirectory != null) {
            log.info("Cleaning up job directory (strategy: {})", cleanupStrategy);
            try {
                this.jobSetupService.cleanupJobDirectory(
                    jobDirectory.toPath(),
                    cleanupStrategy
                );
            } catch (final IOException e) {
                throw new RetryableJobExecutionException("Failed to cleanup job directory", e);
            }
        }
    }
}
