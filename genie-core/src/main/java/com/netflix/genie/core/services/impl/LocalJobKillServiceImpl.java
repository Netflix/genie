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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.events.KillJobEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.util.ProcessChecker;
import com.netflix.genie.core.util.UnixProcessChecker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.context.event.EventListener;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Calendar;

/**
 * Implementation of the JobKillService interface which attempts to kill jobs running on the local node.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LocalJobKillServiceImpl implements JobKillService {

    private final String hostName;
    private final JobSearchService jobSearchService;
    private final Executor executor;
    private final boolean runAsUser;

    /**
     * Constructor.
     *
     * @param hostName         The name of the host this Genie node is running on
     * @param jobSearchService The job search service to use to locate job information
     * @param executor         The executor to use to run system processes
     * @param runAsUser        True if jobs are run as the user who submitted the job
     */
    public LocalJobKillServiceImpl(
        @NotBlank final String hostName,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final Executor executor,
        final boolean runAsUser
    ) {
        this.hostName = hostName;
        this.jobSearchService = jobSearchService;
        this.executor = executor;
        this.runAsUser = runAsUser;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killJob(@NotBlank final String id) throws GenieException {
        // Will throw exception if not found
        // TODO: Could instead check JobMonitorCoordinator eventually for in memory check
        final JobExecution jobExecution = this.jobSearchService.getJobExecution(id);
        if (jobExecution.getExitCode() != JobExecution.DEFAULT_EXIT_CODE) {
            // Job is already finished one way or another
            return;
        }

        if (!this.hostName.equals(jobExecution.getHostName())) {
            throw new GeniePreconditionException(
                "Job with id "
                    + id
                    + " is not running on this host ("
                    + this.hostName
                    + "). It's actually on "
                    + jobExecution.getHostName()
            );
        }

        // Job is on this node and still running as of when query was made to database
        if (SystemUtils.IS_OS_UNIX) {
            this.killJobOnUnix(jobExecution.getProcessId());
        } else {
            // Windows, etc. May support later
            throw new java.lang.UnsupportedOperationException("Genie isn't currently supported on this OS");
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
        this.killJob(event.getId());

        // Do we send a job finished event here? May lead to race conditions with Job monitor, leaving in JobMonitor
        // for now... - TG
    }

    private void killJobOnUnix(final int pid) throws GenieException {
        try {
            // Ensure this process check can't be timed out
            final Calendar tomorrow = Calendar.getInstance(JobConstants.UTC);
            tomorrow.add(Calendar.DAY_OF_YEAR, 1);
            final ProcessChecker processChecker = new UnixProcessChecker(pid, this.executor, tomorrow.getTime());
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
