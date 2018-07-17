/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.events.JobStartedEvent;
import com.netflix.genie.web.jobs.JobExecutionEnvironment;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSubmitterService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the Job Submitter service that runs the job locally on the same host.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LocalJobRunner implements JobSubmitterService {

    private final JobPersistenceService jobPersistenceService;
    private final List<WorkflowTask> jobWorkflowTasks;
    private final Resource baseWorkingDirPath;
    private final GenieEventBus genieEventBus;

    private final Timer overallSubmitTimer;
    private final Timer createJobDirTimer;
    private final Timer createRunScriptTimer;
    private final Timer executeJobTimer;
    private final Timer saveJobExecutionTimer;
    private final Timer publishJobStartedEventTimer;
    private final Timer createInitFailureDetailsFileTimer;

    /**
     * Constructor create the object.
     *
     * @param jobPersistenceService Implementation of the job persistence service
     * @param genieEventBus         The event bus implementation to use
     * @param workflowTasks         List of all the workflow tasks to be executed
     * @param genieWorkingDir       Working directory for genie where it creates jobs directories
     * @param registry              The metrics registry to use
     */
    public LocalJobRunner(
        @NotNull final JobPersistenceService jobPersistenceService,
        @NonNull final GenieEventBus genieEventBus,
        @NotNull final List<WorkflowTask> workflowTasks,
        @NotNull final Resource genieWorkingDir,
        @NotNull final MeterRegistry registry
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.genieEventBus = genieEventBus;
        this.jobWorkflowTasks = workflowTasks;
        this.baseWorkingDirPath = genieWorkingDir;

        // Metrics
        this.overallSubmitTimer = registry.timer("genie.jobs.submit.localRunner.overall.timer");
        this.createJobDirTimer = registry.timer("genie.jobs.submit.localRunner.createJobDir.timer");
        this.createRunScriptTimer = registry.timer("genie.jobs.submit.localRunner.createRunScript.timer");
        this.executeJobTimer = registry.timer("genie.jobs.submit.localRunner.executeJob.timer");
        this.saveJobExecutionTimer = registry.timer("genie.jobs.submit.localRunner.saveJobExecution.timer");
        this.publishJobStartedEventTimer = registry.timer("genie.jobs.submit.localRunner.publishJobStartedEvent.timer");
        this.createInitFailureDetailsFileTimer = registry.timer(
            "genie.jobs.submit.localRunner.createInitFailureDetailsFile.timer"
        );
    }

    /**
     * {@inheritDoc}
     */
    @SuppressFBWarnings(
        value = "REC_CATCH_EXCEPTION",
        justification = "We catch exception to make sure we always mark job failed."
    )
    @Override
    public void submitJob(
        @NotNull(message = "No job provided. Unable to submit job for execution.")
        @Valid final JobRequest jobRequest,
        @NotNull(message = "No cluster provided. Unable to submit job for execution")
        @Valid final Cluster cluster,
        @NotNull(message = "No command provided. Unable to submit job for execution")
        @Valid final Command command,
        @NotNull(message = "No applications provided. Unable to execute") final List<Application> applications,
        @Min(value = 1, message = "Memory can't be less than 1 MB") final int memory
    ) throws GenieException {
        final long start = System.nanoTime();

        try {
            log.info("Beginning local job submission for {}", jobRequest);
            final String id = jobRequest.getId().orElseThrow(() -> new GenieServerException("No job id found."));

            try {
                final File jobWorkingDir = this.createJobWorkingDirectory(id);
                final File runScript = this.createRunScript(jobWorkingDir);

                // The map object stores the context for all the workflow tasks
                final Map<String, Object> context
                    = this.createJobContext(jobRequest, cluster, command, applications, memory, jobWorkingDir);

                // Execute the job
                final JobExecution jobExecution = this.executeJob(context, runScript);

                // Job Execution will be null in local mode.
                if (jobExecution != null) {
                    // Persist the jobExecution information. This also updates jobStatus to Running
                    final long createJobExecutionStart = System.nanoTime();
                    try {
                        log.info("Saving job execution for job {}", jobRequest.getId());
                        this.jobPersistenceService.setJobRunningInformation(
                            id,
                            jobExecution.
                                getProcessId()
                                .orElseThrow(() ->
                                    new GenieServerException("No process id returned. Unable to persist")
                                ),
                            jobExecution
                                .getCheckDelay()
                                .orElse(com.netflix.genie.common.dto.Command.DEFAULT_CHECK_DELAY),
                            jobExecution
                                .getTimeout()
                                .orElseThrow(() ->
                                    new GenieServerException("No timeout date returned. Unable to persist")
                                )
                        );
                    } finally {
                        this.saveJobExecutionTimer
                            .record(System.nanoTime() - createJobExecutionStart, TimeUnit.NANOSECONDS);
                    }

                    // Publish a job start Event
                    final long publishEventStart = System.nanoTime();
                    try {
                        log.info("Publishing job started event for job {}", id);
                        this.genieEventBus.publishSynchronousEvent(new JobStartedEvent(jobExecution, this));
                    } finally {
                        this.publishJobStartedEventTimer
                            .record(System.nanoTime() - publishEventStart, TimeUnit.NANOSECONDS);
                    }
                }
            } catch (final GeniePreconditionException gpe) {
                log.error(gpe.getMessage(), gpe);
                this.createInitFailureDetailsFile(id, gpe);
                this.genieEventBus.publishAsynchronousEvent(
                    new JobFinishedEvent(
                        id, JobFinishedReason.INVALID, JobStatusMessages.SUBMIT_PRECONDITION_FAILURE, this
                    )
                );
                throw gpe;
            } catch (final Exception e) {
                log.error(e.getMessage(), e);
                this.createInitFailureDetailsFile(id, e);
                this.genieEventBus.publishAsynchronousEvent(
                    new JobFinishedEvent(
                        id, JobFinishedReason.FAILED_TO_INIT, JobStatusMessages.SUBMIT_INIT_FAILURE, this
                    )
                );
                throw e;
            }
        } finally {
            this.overallSubmitTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private void createInitFailureDetailsFile(final String id, final Exception e) {
        final long start = System.nanoTime();
        try {
            final File jobDir = new File(this.baseWorkingDirPath.getFile(), id);
            if (jobDir.exists()) {
                final File detailsFile = new File(jobDir, JobConstants.GENIE_INIT_FAILURE_MESSAGE_FILE_NAME);
                final boolean detailsFileExists = !detailsFile.createNewFile();
                if (detailsFileExists) {
                    log.warn("Init failure details file exists");
                }
                try (
                    final PrintWriter p = new PrintWriter(new OutputStreamWriter(
                        new FileOutputStream(detailsFile), StandardCharsets.UTF_8)
                    )
                ) {
                    p.format(" *** Initialization failure for job: %s ***%n"
                            + "%n"
                            + "Exception: %s - %s%n"
                            + "Trace:%n",
                        id, e.getClass().getCanonicalName(), e.getMessage());
                    e.printStackTrace(p);
                }
                log.info("Created init failure details file {}", detailsFile);
            } else {
                log.error("Could not create init failure details file, job directory does not exist");
            }
        } catch (Throwable t) {
            log.error("Failed to create init failure details file", t);
        } finally {
            this.createInitFailureDetailsFileTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private File createJobWorkingDirectory(final String id) throws GenieException {
        final long start = System.nanoTime();
        try {
            final File jobDir = new File(this.baseWorkingDirPath.getFile(), id);
            if (!jobDir.mkdirs()) {
                throw new GenieServerException(
                    "Could not create job working directory directory: " + jobDir.getCanonicalPath()
                );
            }
            log.info("Created job dir {}", jobDir);
            return jobDir;
        } catch (final IOException ioe) {
            throw new GenieServerException("Could not resolve job working directory due to exception", ioe);
        } finally {
            this.createJobDirTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private File createRunScript(final File jobWorkingDir) throws GenieException {
        final long start = System.nanoTime();
        try {
            final File runScript = new File(jobWorkingDir, JobConstants.GENIE_JOB_LAUNCHER_SCRIPT);
            if (!runScript.exists()) {
                try {
                    if (!runScript.createNewFile()) {
                        throw new GenieServerException("Unable to create run script file due to unknown reason.");
                    }
                } catch (final IOException ioe) {
                    throw new GenieServerException("Unable to create run script file due to IOException.", ioe);
                }
            }
            if (!runScript.setExecutable(true)) {
                throw new GenieServerException("Unable to make run script executable");
            }
            log.info("Created run script {}", runScript);
            return runScript;
        } finally {
            this.createRunScriptTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Map<String, Object> createJobContext(
        final JobRequest jobRequest,
        final Cluster cluster,
        final Command command,
        final List<Application> applications,
        final int memory,
        final File jobWorkingDir
    ) throws GenieException {
        // construct the job execution environment object for this job request
        final JobExecutionEnvironment jee = new JobExecutionEnvironment.Builder(
            jobRequest,
            cluster,
            command,
            memory,
            jobWorkingDir
        )
            .withApplications(applications)
            .build();

        // The map object stores the context for all the workflow tasks
        final Map<String, Object> context = new HashMap<>();

        context.put(JobConstants.JOB_EXECUTION_ENV_KEY, jee);

        return context;
    }

    private JobExecution executeJob(final Map<String, Object> context, final File runScript) throws GenieException {
        final long start = System.nanoTime();
        try (final Writer writer = new OutputStreamWriter(new FileOutputStream(runScript), StandardCharsets.UTF_8)) {
            final String jobId = ((JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY))
                .getJobRequest()
                .getId()
                .orElseThrow(() -> new GenieServerException("No job id. Unable to execute"));
            log.info("Executing job workflow for job {}", jobId);
            context.put(JobConstants.WRITER_KEY, writer);

            for (WorkflowTask workflowTask : this.jobWorkflowTasks) {
                workflowTask.executeTask(context);
                if (Thread.currentThread().isInterrupted()) {
                    log.info("Interrupted job workflow for job {}", jobId);
                    break;
                }
            }

            log.info("Finished Executing job workflow for job {}", jobId);
            return (JobExecution) context.get(JobConstants.JOB_EXECUTION_DTO_KEY);
        } catch (final IOException ioe) {
            throw new GenieServerException("Failed to execute job due to: " + ioe.getMessage(), ioe);
        } finally {
            this.executeJobTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
