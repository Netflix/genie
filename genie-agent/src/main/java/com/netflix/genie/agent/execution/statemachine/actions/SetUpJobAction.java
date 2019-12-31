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
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.internal.dtos.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Action performed when in state SETUP_JOB.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class SetUpJobAction extends BaseStateAction implements StateAction.SetUpJob {

    private final JobSetupService jobSetupService;
    private final AgentJobService agentJobService;
    private final AgentHeartBeatService heartbeatService;
    private final AgentJobKillService killService;
    private final AgentFileStreamService agentFileStreamService;

    SetUpJobAction(
        final ExecutionContext executionContext,
        final JobSetupService jobSetupService,
        final AgentJobService agentJobService,
        final AgentHeartBeatService heartbeatService,
        final AgentJobKillService killService,
        final AgentFileStreamService fileStreamService
    ) {
        super(executionContext);
        this.jobSetupService = jobSetupService;
        this.agentJobService = agentJobService;
        this.heartbeatService = heartbeatService;
        this.killService = killService;
        this.agentFileStreamService = fileStreamService;
    }

    @Override
    protected void executePreActionValidation() {
        assertClaimedJobIdPresent();
        assertCurrentJobStatusEqual(JobStatus.CLAIMED);
        assertJobSpecificationPresent();
        //TODO assert claimed ID matches spec ID
        assertJobDirectoryNotPresent();
        assertJobEnvironmentNotPresent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(
        final ExecutionContext executionContext
    ) {
        UserConsole.getLogger().info("Setting up job...");

        final JobSpecification jobSpecification = executionContext.getJobSpecification().get();
        final String claimedJobId = jobSpecification.getJob().getId();

        this.heartbeatService.start(claimedJobId);
        this.killService.start(claimedJobId);

        try {
            // Create job directory
            final File jobDirectory;
            jobDirectory = this.jobSetupService.createJobDirectory(jobSpecification);
            executionContext.setJobDirectory(jobDirectory);

            // Move the agent log file inside the job folder
            relocateAgentLogFile(jobDirectory);

            // Start manifest service, allowing server to browse and request files.
            this.agentFileStreamService.start(claimedJobId, jobDirectory.toPath());

            // Set status to INIT
            this.agentJobService.changeJobStatus(
                claimedJobId,
                JobStatus.CLAIMED,
                JobStatus.INIT,
                JobStatusMessages.JOB_INITIALIZING
            );
            executionContext.setCurrentJobStatus(JobStatus.INIT);

            // Download dependencies, configurations, etc.
            final List<File> setupFiles = this.jobSetupService.downloadJobResources(jobSpecification, jobDirectory);

            final Map<String, String> jobEnvironment = this.jobSetupService.setupJobEnvironment(
                jobDirectory,
                jobSpecification,
                setupFiles
            );
            executionContext.setJobEnvironment(jobEnvironment);

        } catch (SetUpJobException e) {
            throw new RuntimeException("Failed to set up job directory and environment", e);
        } catch (final ChangeJobStatusException e) {
            throw new RuntimeException("Failed set job status to INIT", e);
        }

        this.agentFileStreamService.forceServerSync();

        return Events.SETUP_JOB_COMPLETE;
    }

    @Override
    protected void executePostActionValidation() {
        assertCurrentJobStatusEqual(JobStatus.INIT);
        assertJobDirectoryPresent();
        assertJobEnvironmentPresent();
    }

    @Override
    protected void executeStateActionCleanup(final ExecutionContext executionContext) {
        // Stop services started during setup
        killService.stop();
        heartbeatService.stop();
        agentFileStreamService.stop();
    }

    private void relocateAgentLogFile(final File jobDirectory) {
        final Path destinationPath = PathUtils.jobAgentLogFilePath(jobDirectory);
        log.info("Relocating agent log file to: {}", destinationPath);
        try {
            UserConsole.relocateLogFile(destinationPath);
        } catch (IOException e) {
            log.error("Failed to relocate agent log file", e);
        }
    }
}
