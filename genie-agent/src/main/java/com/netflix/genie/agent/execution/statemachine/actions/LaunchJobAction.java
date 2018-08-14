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

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.exceptions.JobLaunchException;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.LaunchJobService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Action performed when in state LAUNCH_JOB.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class LaunchJobAction extends BaseStateAction implements StateAction.LaunchJob {

    private final LaunchJobService launchJobService;
    private final AgentJobService agentJobService;

    LaunchJobAction(
        final ExecutionContext executionContext,
        final LaunchJobService launchJobService,
        final AgentJobService agentJobService
    ) {
        super(executionContext);
        this.launchJobService = launchJobService;
        this.agentJobService = agentJobService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        log.info("Launching job...");

        final JobSpecification jobSpec = executionContext.getJobSpecification();
        final File jobRunDirectory = executionContext.getJobDirectory();
        final Map<String, String> jobEnvironment = executionContext.getJobEnvironment();
        final List<String> jobCommandLine = jobSpec.getCommandArgs();
        final boolean interactive = jobSpec.isInteractive();
        final JobStatus currentJobStatus = executionContext.getCurrentJobStatus();

        try {
            launchJobService.launchProcess(
                jobRunDirectory,
                jobEnvironment,
                jobCommandLine,
                interactive
            );
        } catch (final JobLaunchException e) {
            throw new RuntimeException("Failed to launch job", e);
        }

        try {
            this.agentJobService.changeJobStatus(
                executionContext.getClaimedJobId(),
                currentJobStatus,
                JobStatus.RUNNING,
                "Job process launched"
            );
            executionContext.setCurrentJobStatus(JobStatus.RUNNING);
        } catch (final ChangeJobStatusException e) {
            throw new RuntimeException("Failed to update job status", e);
        }

        return Events.LAUNCH_JOB_COMPLETE;
    }
}
