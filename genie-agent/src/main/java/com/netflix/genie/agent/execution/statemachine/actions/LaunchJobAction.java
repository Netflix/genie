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
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Action performed when in state LAUNCH_JOB.
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

        final Process jobProcess;
        try {
            jobProcess = launchJobService.launchProcess(
                jobRunDirectory,
                jobEnvironment,
                jobCommandLine,
                interactive
            );
        } catch (final JobLaunchException e) {
            throw new RuntimeException("Failed to launch job", e);
        }

        final long pid = getPid(jobProcess);

        log.info("Job process started (pid: {})", pid);

        executionContext.setJobProcess(jobProcess);

        try {
            this.agentJobService.changeJobStatus(
                executionContext.getClaimedJobId(),
                executionContext.getCurrentJobStatus(),
                JobStatus.RUNNING,
                "Job running (pid: " + pid + ")"
            );
            executionContext.setCurrentJobStatus(JobStatus.RUNNING);

        } catch (final ChangeJobStatusException e) {
            throw new RuntimeException("Failed to update job status", e);
        }

        return Events.LAUNCH_JOB_COMPLETE;
    }

    /* TODO: HACK, Process does not expose PID in Java 8 API */
    private long getPid(final Process process) {
        long pid = -1;
        final String processClassName = process.getClass().getCanonicalName();
        try {
            if ("java.lang.UNIXProcess".equals(processClassName)) {
                final Field pidMemberField = process.getClass().getDeclaredField("pid");
                final boolean resetAccessible = pidMemberField.isAccessible();
                pidMemberField.setAccessible(true);
                pid = pidMemberField.getLong(process);
                pidMemberField.setAccessible(resetAccessible);
            } else {
                log.debug("Don't know how to access PID for class {}", processClassName);
            }
        } catch (final Throwable t) {
            log.warn("Failed to determine job process PID");
        }
        return pid;
    }
}
