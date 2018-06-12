/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.services.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.events.KillJobEvent;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.jobs.JobKillReasonFile;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.util.ProcessChecker;
import com.netflix.genie.web.util.UnixProcessChecker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Implementation of the JobKillService interface which attempts to kill jobs running on the local node.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LocalJobKillServiceImpl implements JobKillService {

    private final String hostname;
    private final JobSearchService jobSearchService;
    private final Executor executor;
    private final boolean runAsUser;
    private final GenieEventBus genieEventBus;
    private final File baseWorkingDir;
    private final ObjectMapper objectMapper;

    /**
     * Constructor.
     *
     * @param hostname         The name of the host this Genie node is running on
     * @param jobSearchService The job search service to use to locate job information
     * @param executor         The executor to use to run system processes
     * @param runAsUser        True if jobs are run as the user who submitted the job
     * @param genieEventBus    The system event bus to use
     * @param genieWorkingDir  The working directory where all job directories are created.
     * @param objectMapper     The Jackson ObjectMapper used to serialize from/to JSON
     */
    public LocalJobKillServiceImpl(
        @NotBlank final String hostname,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final Executor executor,
        final boolean runAsUser,
        @NotNull final GenieEventBus genieEventBus,
        @NotNull final Resource genieWorkingDir,
        @NotNull final ObjectMapper objectMapper
    ) {
        this.hostname = hostname;
        this.jobSearchService = jobSearchService;
        this.executor = executor;
        this.runAsUser = runAsUser;
        this.genieEventBus = genieEventBus;
        this.objectMapper = objectMapper;

        try {
            this.baseWorkingDir = genieWorkingDir.getFile();
        } catch (IOException gse) {
            throw new RuntimeException("Could not load the base path from resource", gse);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killJob(
        @NotBlank(message = "No id entered. Unable to kill job.") final String id,
        @NotBlank(message = "No reason provided.") final String reason
    ) throws GenieException {
        // Will throw exception if not found
        // TODO: Could instead check JobMonitorCoordinator eventually for in memory check
        final JobStatus jobStatus = this.jobSearchService.getJobStatus(id);
        if (jobStatus == JobStatus.INIT) {
            // Send a job finished event to force system to update the job to killed
            this.genieEventBus.publishSynchronousEvent(
                new JobFinishedEvent(
                    id,
                    JobFinishedReason.KILLED,
                    JobStatusMessages.USER_REQUESTED_JOB_BE_KILLED_DURING_INITIALIZATION,
                    this
                )
            );
        } else if (jobStatus == JobStatus.RUNNING) {
            final JobExecution jobExecution = this.jobSearchService.getJobExecution(id);
            if (jobExecution.getExitCode().isPresent()) {
                // Job is already finished one way or another
                return;
            }

            if (!this.hostname.equals(jobExecution.getHostName())) {
                throw new GeniePreconditionException(
                    "Job with id "
                        + id
                        + " is not running on this host ("
                        + this.hostname
                        + "). It's actually on "
                        + jobExecution.getHostName()
                );
            }

            // Job is on this node and still running as of when query was made to database
            if (SystemUtils.IS_OS_UNIX) {
                this.killJobOnUnix(jobExecution
                    .getProcessId()
                    .orElseThrow(() ->
                        new GeniePreconditionException("No process id found. Unable to kill if no process id")
                    )
                );
            } else {
                // Windows, etc. May support later
                throw new UnsupportedOperationException("Genie isn't currently supported on this OS");
            }

            // Write additional file with kill reason
            try {
                this.objectMapper.writeValue(
                    new File(this.baseWorkingDir + "/"
                        + id + "/"
                        + JobConstants.GENIE_KILL_REASON_FILE_NAME),
                    new JobKillReasonFile(reason));
            } catch (IOException e) {
                throw new GenieServerException("Failed to write job kill reason file", e);
            }
        }
    }

    /**
     * Listen for job kill events from within the system as opposed to on calls from users directly to killJob.
     *
     * @param event The {@link KillJobEvent}
     * @throws GenieException On error
     */
    @EventListener
    public void onKillJobEvent(@NotNull final KillJobEvent event) throws GenieException {
        this.killJob(event.getId(), event.getReason());
    }

    private void killJobOnUnix(final int pid) throws GenieException {
        try {
            // Ensure this process check can't be timed out
            final Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
            final ProcessChecker processChecker = new UnixProcessChecker(pid, this.executor, tomorrow);
            processChecker.checkProcess();
        } catch (final ExecuteException ee) {
            // This means the job was done already
            log.debug("Process with pid {} is already done", pid);
            return;
        } catch (final IOException ioe) {
            throw new GenieServerException("Unable to check process status for pid " + pid, ioe);
        }

        // TODO: Do we need retries?
        // This means the job client process is still running
        try {
            final CommandLine killCommand;
            if (this.runAsUser) {
                killCommand = new CommandLine("sudo");
                killCommand.addArgument("kill");
            } else {
                killCommand = new CommandLine("kill");
            }
            killCommand.addArguments(Integer.toString(pid));
            this.executor.execute(killCommand);
        } catch (final IOException ioe) {
            throw new GenieServerException("Unable to kill process " + pid, ioe);
        }
    }
}
