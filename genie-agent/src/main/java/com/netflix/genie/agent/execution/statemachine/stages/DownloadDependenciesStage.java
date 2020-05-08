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
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Set;

/**
 * Download dependencies such as binaries and configurations attached to the job and its dependent entities.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class DownloadDependenciesStage extends ExecutionStage {
    private final JobSetupService jobSetupService;

    /**
     * Constructor.
     *
     * @param jobSetupService job setup service
     */
    public DownloadDependenciesStage(final JobSetupService jobSetupService) {
        super(States.DOWNLOAD_DEPENDENCIES);
        this.jobSetupService = jobSetupService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final JobSpecification jobSpecification = executionContext.getJobSpecification();
        final File jobDirectory = executionContext.getJobDirectory();

        assert jobSpecification != null;
        assert jobDirectory != null;

        log.info("Downloading job dependencies");
        final Set<File> downloaded;
        try {
            downloaded = this.jobSetupService.downloadJobResources(jobSpecification, jobDirectory);
        } catch (SetUpJobException e) {
            throw createFatalException(e);
        }

        ConsoleLog.getLogger().info("Downloaded dependencies ({} files)", downloaded.size());

    }
}
