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

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Action performed when in state MONITOR_JOB.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class MonitorJobAction extends BaseStateAction implements StateAction.MonitorJob {

    private final AgentJobService agentJobService;
    private final JobProcessManager jobProcessManager;
    private final JobSetupService jobSetupService;
    private final ArgumentDelegates.CleanupArguments cleanupArguments;
    private final JobArchiveService jobArchiveService;

    MonitorJobAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService,
        final JobProcessManager jobProcessManager,
        final JobSetupService jobSetupService,
        final JobArchiveService jobArchiveService,
        final ArgumentDelegates.CleanupArguments cleanupArguments
    ) {
        super(executionContext);
        this.agentJobService = agentJobService;
        this.jobProcessManager = jobProcessManager;
        this.jobSetupService = jobSetupService;
        this.jobArchiveService = jobArchiveService;
        this.cleanupArguments = cleanupArguments;
    }

    @Override
    protected void executePreActionValidation() {
        assertClaimedJobIdPresent();
        assertCurrentJobStatusEqual(JobStatus.RUNNING);
        assertFinalJobStatusNotPresent();
        assertJobSpecificationPresent();
        assertJobDirectoryPresent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        UserConsole.getLogger().info("Monitoring job...");

        final JobProcessResult jobProcessResult;
        try {
            jobProcessResult = this.jobProcessManager.waitFor();
        } catch (final InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting for job process completion", e);
        }

        final JobStatus finalJobStatus = jobProcessResult.getFinalStatus();
        log.info("Job process completed with final status {}", finalJobStatus);

        final File jobDirectory = executionContext.getJobDirectory().get();
        final JobSpecification jobSpecification = executionContext.getJobSpecification().get();

        // Cleanup job directory before archiving it
        cleanupJobDirectory(jobDirectory);

        // Archive job outputs before setting the final (non-active) status.
        // Active status is used by the server to determine wether to route the request to archival or a running agent.
        archiveJobDirectory(jobDirectory, jobSpecification);

        try {
            this.agentJobService.changeJobStatus(
                executionContext.getClaimedJobId().get(),
                JobStatus.RUNNING,
                finalJobStatus,
                jobProcessResult.getFinalStatusMessage()
            );
            executionContext.setCurrentJobStatus(finalJobStatus);
            executionContext.setFinalJobStatus(finalJobStatus);
        } catch (ChangeJobStatusException e) {
            throw new RuntimeException("Failed to update job status", e);
        }

        return Events.MONITOR_JOB_COMPLETE;
    }

    @Override
    protected void executePostActionValidation() {
        assertFinalJobStatusPresentAndValid();
    }

    private void archiveJobDirectory(
        final File jobDirectory,
        final JobSpecification jobSpecification
    ) {
        final Optional<String> archiveLocationOptional = jobSpecification.getArchiveLocation();
        if (archiveLocationOptional.isPresent()) {
            final String archiveLocation = archiveLocationOptional.get();
            if (StringUtils.isNotBlank(archiveLocation)) {
                try {
                    log.info("Attempting to archive job folder to: " + archiveLocation);
                    this.jobArchiveService.archiveDirectory(
                        jobDirectory.toPath(),
                        new URI(archiveLocation)
                    );
                    log.info("Job folder archived to: " + archiveLocation);
                } catch (JobArchiveException | URISyntaxException e) {
                    log.error("Error archiving job folder", e);
                }
            }
        }
    }

    private void cleanupJobDirectory(
        final File jobDirectory
    ) {
        try {
            this.jobSetupService.cleanupJobDirectory(
                jobDirectory.toPath(),
                cleanupArguments.getCleanupStrategy()
            );
        } catch (final IOException e) {
            log.warn("Exception while performing job directory cleanup", e);
        }
    }
}
