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
import com.netflix.genie.agent.execution.exceptions.ChangeJobArchiveStatusException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Archive job output files and logs, if the job reached a state where it is appropriate to do so.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ArchiveJobOutputsStage extends ExecutionStage {
    private final JobArchiveService jobArchiveService;
    private final AgentJobService agentJobService;

    /**
     * Constructor.
     *
     * @param jobArchiveService job archive service
     * @param agentJobService   agent job service
     */
    public ArchiveJobOutputsStage(final JobArchiveService jobArchiveService, final AgentJobService agentJobService) {
        super(States.ARCHIVE);
        this.jobArchiveService = jobArchiveService;
        this.agentJobService = agentJobService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final JobSpecification jobSpecification = executionContext.getJobSpecification();
        final File jobDirectory = executionContext.getJobDirectory();

        if (jobSpecification != null && jobDirectory != null) {
            final String archiveLocation = jobSpecification.getArchiveLocation().orElse(null);
            if (StringUtils.isNotBlank(archiveLocation)) {

                boolean success = false;
                try {
                    log.info("Archive job folder to: " + archiveLocation);
                    this.jobArchiveService.archiveDirectory(
                        jobDirectory.toPath(),
                        new URI(archiveLocation)
                    );
                    success = true;
                } catch (JobArchiveException | URISyntaxException e) {
                    // Swallow the error and move on.
                    log.error("Error archiving job folder", e);
                    ConsoleLog.getLogger().error(
                        "Job file archive error: {}",
                        e.getCause() != null ? e.getCause().getMessage() : e.getMessage()
                    );
                }

                final String jobId = executionContext.getClaimedJobId();
                final ArchiveStatus archiveStatus = success ? ArchiveStatus.ARCHIVED : ArchiveStatus.FAILED;
                try {
                    this.agentJobService.changeJobArchiveStatus(jobId, archiveStatus);
                } catch (ChangeJobArchiveStatusException e) {
                    // Swallow the error and move on.
                    log.error("Error updating the archive status", e);
                }
            }
        }
    }
}

