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
package com.netflix.genie.web.tasks.job;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.jobs.JobDoneFile;
import com.netflix.genie.web.jobs.JobKillReasonFile;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.MailService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.Resource;
import org.springframework.retry.support.RetryTemplate;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A class that has the methods to perform various tasks when a job completes.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobCompletionService {

    static final String JOB_COMPLETION_TIMER_NAME = "genie.jobs.completion.timer";
    static final String JOB_COMPLETION_ERROR_COUNTER_NAME = "genie.jobs.errors.count";
    static final String ERROR_SOURCE_TAG = "error";
    static final String JOB_FINAL_STATE = "jobFinalState";
    private final JobPersistenceService jobPersistenceService;
    private final JobSearchService jobSearchService;
    private final GenieFileTransferService genieFileTransferService;
    private final File baseWorkingDir;
    private final MailService mailServiceImpl;
    private final Executor executor;
    private final boolean deleteArchiveFile;
    private final boolean deleteDependencies;
    private final boolean runAsUserEnabled;

    // Metrics
    private final MeterRegistry registry;
    private final RetryTemplate retryTemplate;

    /**
     * Constructor.
     *
     * @param jobSearchService         An implementation of the job search service.
     * @param jobPersistenceService    An implementation of the job persistence service.
     * @param genieFileTransferService An implementation of the Genie File Transfer service.
     * @param genieWorkingDir          The working directory where all job directories are created.
     * @param mailServiceImpl          An implementation of the mail service.
     * @param registry                 The metrics registry to use
     * @param jobsProperties           The properties relating to running jobs
     * @param retryTemplate            Retry template for retrying remote calls
     * @throws GenieException if there is a problem
     */
    public JobCompletionService(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final GenieFileTransferService genieFileTransferService,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final MailService mailServiceImpl,
        final MeterRegistry registry,
        final JobsProperties jobsProperties,
        @Qualifier("genieRetryTemplate") @NotNull final RetryTemplate retryTemplate
    ) throws GenieException {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.genieFileTransferService = genieFileTransferService;
        this.mailServiceImpl = mailServiceImpl;
        this.deleteArchiveFile = jobsProperties.getCleanup().isDeleteArchiveFile();
        this.deleteDependencies = jobsProperties.getCleanup().isDeleteDependencies();
        this.runAsUserEnabled = jobsProperties.getUsers().isRunAsUserEnabled();

        this.executor = new DefaultExecutor();
        this.executor.setStreamHandler(new PumpStreamHandler(null, null));

        try {
            this.baseWorkingDir = genieWorkingDir.getFile();
        } catch (IOException gse) {
            throw new GenieServerException("Could not load the base path from resource", gse);
        }

        // Set up the metrics
        this.registry = registry;
        // Retry template
        this.retryTemplate = retryTemplate;
    }

    /**
     * Event listener for when a job is completed. Updates the status of the job.
     *
     * @param event The Spring Boot application ready event to startup on
     */
    void handleJobCompletion(final JobFinishedEvent event) {
        final long start = System.nanoTime();
        final String jobId = event.getId();
        final Set<Tag> tags = Sets.newHashSet();

        try {
            final Job job = this.retryTemplate.execute(context -> this.getJob(jobId));

            final JobStatus status = job.getStatus();

            // Make sure the job isn't already done before doing something
            if (status.isActive()) {
                try {
                    this.retryTemplate.execute(context -> this.updateJob(job, event, tags));
                } catch (final Exception e) {
                    log.error("Failed updating for job: {}", jobId, e);
                }
                // Things that should be done either way
                try {
                    this.retryTemplate.execute(context -> this.processJobDir(job));
                } catch (final Exception e) {
                    log.error("Failed archiving directory for job: {}", jobId, e);
                    this.incrementErrorCounter("JOB_DIRECTORY_FAILURE", e);
                }
                try {
                    this.retryTemplate.execute(context -> sendEmail(jobId));
                } catch (final Exception e) {
                    log.error("Failed sending email for job: {}", jobId, e);
                    this.incrementErrorCounter("JOB_UPDATE_FAILURE", e);
                }
            }
            MetricsUtils.addSuccessTags(tags);
        } catch (final Exception e) {
            log.error("Failed getting job with id: {}", jobId, e);
            MetricsUtils.addFailureTagsWithException(tags, e);
        } finally {
            this.registry
                .timer(JOB_COMPLETION_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Job getJob(final String jobId) throws GenieException {
        return this.jobSearchService.getJob(jobId);
    }

    private Void updateJob(
        final Job job,
        final JobFinishedEvent event,
        final Set<Tag> tags
    ) throws GenieException {
        try {
            final String jobId = event.getId();
            final JobStatus status = job.getStatus();
            // Now we know this job should be marked in one of the finished states
            JobStatus eventStatus = null;
            if (status == JobStatus.INIT) {
                switch (event.getReason()) {
                    case KILLED:
                        eventStatus = JobStatus.KILLED;
                        break;
                    case INVALID:
                        eventStatus = JobStatus.INVALID;
                        break;
                    case FAILED_TO_INIT:
                        eventStatus = JobStatus.FAILED;
                        break;
                    case PROCESS_COMPLETED:
                        eventStatus = JobStatus.SUCCEEDED;
                        break;
                    case SYSTEM_CRASH:
                        eventStatus = JobStatus.FAILED;
                        break;
                    default:
                        eventStatus = JobStatus.INVALID;
                        log.warn("Unknown event status for job: {}", jobId);
                }
            } else {
                if (event.getReason() != JobFinishedReason.SYSTEM_CRASH) {
                    try {
                        final String finalStatus =
                            this.retryTemplate.execute(context -> updateFinalStatusForJob(jobId).toString());
                        tags.add(Tag.of(JOB_FINAL_STATE, finalStatus));
                        cleanupProcesses(jobId);
                    } catch (Exception e) {
                        log.error("Failed updating the exit code and status for job: {}", jobId, e);
                    }
                } else {
                    tags.add(Tag.of(JOB_FINAL_STATE, JobStatus.FAILED.toString()));
                    eventStatus = JobStatus.FAILED;
                }
            }

            if (eventStatus != null) {
                tags.add(Tag.of(JOB_FINAL_STATE, status.toString()));
                this.jobPersistenceService.updateJobStatus(jobId, eventStatus, event.getMessage());
            }
        } catch (Throwable t) {
            incrementErrorCounter("JOB_UPDATE_FAILURE", t);
            throw t;
        }
        return null;
    }

    /**
     * An external fail-safe mechanism to clean up processes left behind by the run.sh after the
     * job is killed or failed. This method is a no-op for jobs whose status is INVALID.
     *
     * @param jobId The id of the job to cleanup processes for.
     */
    private void cleanupProcesses(final String jobId) {
        try {
            if (!this.jobSearchService.getJobStatus(jobId).equals(JobStatus.INVALID)) {
                this.jobSearchService.getJobExecution(jobId).getProcessId().ifPresent(pid -> {
                    try {
                        final CommandLine commandLine = new CommandLine(JobConstants.UNIX_PKILL_COMMAND);
                        commandLine.addArgument(JobConstants.getKillFlag());
                        commandLine.addArgument(Integer.toString(pid));
                        this.executor.execute(commandLine);

                        // The process group should not exist and the above code should always throw and exception.
                        // If it does not then the bash script is not cleaning up stuff well during kills
                        // or the script is done but child processes are still remaining. This metric tracks all that.
                        incrementErrorCounter("JOB_PROCESS_CLEANUP_NOT_THROWING_FAILURE", new RuntimeException());
                    } catch (final Exception e) {
                        log.debug("Received expected exception. Ignoring.");
                    }
                });
            }
        } catch (final GenieException ge) {
            log.error("Unable to cleanup process for job {} due to exception.", jobId, ge);
            incrementErrorCounter("JOB_CLEANUP_FAILURE", ge);
        } catch (Throwable t) {
            incrementErrorCounter("JOB_PROCESS_CLEANUP_FAILURE", t);
            throw t;
        }
    }

    /**
     * Updates the status of the job.
     *
     * @param id The job id.
     * @return the final job status
     * @throws GenieException If there is any problem
     */
    private JobStatus updateFinalStatusForJob(final String id) throws GenieException {
        log.debug("Updating the status of the job.");

        try {
            final File jobDir = new File(this.baseWorkingDir, id);
            final JobDoneFile jobDoneFile = GenieObjectMapper.getMapper().readValue(
                new File(this.baseWorkingDir + "/" + id + "/" + JobConstants.GENIE_DONE_FILE_NAME),
                JobDoneFile.class
            );

            final String killedStatusMessages;
            final File killReasonFile = new File(this.baseWorkingDir + "/"
                + id + "/"
                + JobConstants.GENIE_KILL_REASON_FILE_NAME);
            if (killReasonFile.exists()) {
                killedStatusMessages = GenieObjectMapper.getMapper().readValue(
                    killReasonFile,
                    JobKillReasonFile.class
                ).getKillReason();
            } else {
                killedStatusMessages = JobStatusMessages.JOB_KILLED_BY_USER;
            }
            final int exitCode = jobDoneFile.getExitCode();
            // Read the size of STD OUT and STD ERR files
            final File stdOut = new File(jobDir, JobConstants.STDOUT_LOG_FILE_NAME);
            final Long stdOutSize = stdOut.exists() && stdOut.isFile() ? stdOut.length() : null;
            final File stdErr = new File(jobDir, JobConstants.STDERR_LOG_FILE_NAME);
            final Long stdErrSize = stdErr.exists() && stdErr.isFile() ? stdErr.length() : null;
            final JobStatus finalStatus;
            switch (exitCode) {
                case JobExecution.KILLED_EXIT_CODE:
                    this.jobPersistenceService.setJobCompletionInformation(
                        id,
                        exitCode,
                        JobStatus.KILLED,
                        killedStatusMessages,
                        stdOutSize,
                        stdErrSize
                    );
                    finalStatus = JobStatus.KILLED;
                    break;
                case JobExecution.SUCCESS_EXIT_CODE:
                    this.jobPersistenceService.setJobCompletionInformation(
                        id,
                        exitCode,
                        JobStatus.SUCCEEDED,
                        JobStatusMessages.JOB_FINISHED_SUCCESSFULLY,
                        stdOutSize,
                        stdErrSize
                    );
                    finalStatus = JobStatus.SUCCEEDED;
                    break;
                // catch all for non-zero and non-zombie, killed and failed exit codes
                default:
                    this.jobPersistenceService.setJobCompletionInformation(
                        id,
                        exitCode,
                        JobStatus.FAILED,
                        JobStatusMessages.JOB_FAILED,
                        stdOutSize,
                        stdErrSize
                    );
                    finalStatus = JobStatus.FAILED;
                    break;
            }
            return finalStatus;
        } catch (final IOException ioe) {
            incrementErrorCounter("JOB_FINAL_UPDATE_FAILURE", ioe);
            // The run.sh should theoretically ALWAYS generate a done file so we should never hit this code.
            // But if we do handle it generate a metric for it which we can track
            log.error("Could not load the done file for job {}. Marking it as failed.", id, ioe);
            this.jobPersistenceService.updateJobStatus(
                id,
                JobStatus.FAILED,
                JobStatusMessages.COULD_NOT_LOAD_DONE_FILE
            );
            return JobStatus.FAILED;
        } catch (Throwable t) {
            incrementErrorCounter("JOB_FINAL_UPDATE_FAILURE", t);
            throw t;
        }
    }


    /**
     * Delete application, cluster, command dependencies from the job working directory to save space.
     *
     * @param jobId  The ID of the job to delete dependencies for
     * @param jobDir The job working directory
     */
    private void deleteDependenciesDirectories(final String jobId, final File jobDir) {
        log.debug("Deleting dependencies.");

        if (jobDir.exists()) {

            final Collection<File> dependencyDirectories = Sets.newHashSet();

            // Collect application dependencies
            try {
                for (Application application : this.jobSearchService.getJobApplications(jobId)) {
                    application.getId().ifPresent(
                        appId ->
                            dependencyDirectories.add(
                                new File(
                                    jobDir,
                                    JobConstants.GENIE_PATH_VAR
                                        + JobConstants.FILE_PATH_DELIMITER
                                        + JobConstants.APPLICATION_PATH_VAR
                                        + JobConstants.FILE_PATH_DELIMITER
                                        + appId
                                        + JobConstants.FILE_PATH_DELIMITER
                                        + JobConstants.DEPENDENCY_FILE_PATH_PREFIX
                                )
                            )
                    );
                }
            } catch (GenieException e) {
                log.error(
                    "Error collecting application dependencies for job: {} due to error {}",
                    jobId,
                    e.toString());
                incrementErrorCounter("DELETE_APPLICATION_DEPENDENCIES_FAILURE", e);
            }

            // Collect cluster dependencies
            try {
                this.jobSearchService.getJobCluster(jobId).getId().ifPresent(
                    clusterId ->
                        dependencyDirectories.add(
                            new File(
                                jobDir,
                                JobConstants.GENIE_PATH_VAR
                                    + JobConstants.FILE_PATH_DELIMITER
                                    + JobConstants.CLUSTER_PATH_VAR
                                    + JobConstants.FILE_PATH_DELIMITER
                                    + clusterId
                                    + JobConstants.FILE_PATH_DELIMITER
                                    + JobConstants.DEPENDENCY_FILE_PATH_PREFIX
                            )
                        )
                );
            } catch (GenieException e) {
                log.error(
                    "Error collecting cluster dependency for job: {} due to error {}",
                    jobId,
                    e.toString());
                incrementErrorCounter("DELETE_CLUSTER_DEPENDENCIES_FAILURE", e);
            }

            // Collect command dependencies
            try {
                this.jobSearchService.getJobCommand(jobId).getId().ifPresent(
                    commandId ->
                        dependencyDirectories.add(
                            new File(
                                jobDir,
                                JobConstants.GENIE_PATH_VAR
                                    + JobConstants.FILE_PATH_DELIMITER
                                    + JobConstants.COMMAND_PATH_VAR
                                    + JobConstants.FILE_PATH_DELIMITER
                                    + commandId
                                    + JobConstants.FILE_PATH_DELIMITER
                                    + JobConstants.DEPENDENCY_FILE_PATH_PREFIX
                            )
                        )
                );
            } catch (GenieException e) {
                log.error(
                    "Error collecting command dependency for job: {} due to error {}",
                    jobId,
                    e.toString());
                incrementErrorCounter("DELETE_COMMAND_DEPENDENCIES_FAILURE", e);
            }

            // Delete all dependencies
            for (File dependencyDirectory : dependencyDirectories) {
                if (dependencyDirectory.exists()) {
                    try {
                        if (this.runAsUserEnabled) {
                            final CommandLine deleteCommand = new CommandLine("sudo");
                            deleteCommand.addArgument("rm");
                            deleteCommand.addArgument("-rf");
                            deleteCommand.addArgument(dependencyDirectory.getCanonicalPath());
                            log.debug("Delete command is {}", deleteCommand);
                            this.executor.execute(deleteCommand);
                        } else {
                            FileUtils.deleteDirectory(dependencyDirectory);
                        }
                    } catch (IOException e) {
                        incrementErrorCounter("DELETE_DEPENDENCIES_FAILURE");
                        log.error(
                            "Error deleting dependency directory: {}: {}",
                            dependencyDirectory.getAbsolutePath(),
                            e.toString()
                        );
                    } catch (Throwable t) {
                        incrementErrorCounter("DELETE_DEPENDENCIES_FAILURE");
                        throw t;
                    }
                }
            }
        }
    }

    /**
     * Uploads the job directory to the archive location.
     *
     * @param job The job.
     * @throws GenieException if there is any problem
     */
    private boolean processJobDir(final Job job) throws GenieException, IOException {
        log.debug("Got a job finished event. Will process job directory.");
        boolean result = false;
        final Optional<String> oJobId = job.getId();

        // The deletion of dependencies and archiving only happens for job requests which are not Invalid.
        if (oJobId.isPresent() && !(this.jobSearchService.getJobStatus(oJobId.get()).equals(JobStatus.INVALID))) {
            final String jobId = oJobId.get();
            final File jobDir = new File(this.baseWorkingDir, jobId);

            if (jobDir.exists()) {
                if (this.deleteDependencies) {
                    this.deleteDependenciesDirectories(jobId, jobDir);
                }

                final Optional<String> archiveLocation = job.getArchiveLocation();
                if (archiveLocation.isPresent() && !Strings.isNullOrEmpty(archiveLocation.get())) {
                    log.debug("Archiving job directory");
                    // Create the tar file
                    final File localArchiveFile = new File(jobDir, "genie/logs/" + jobId + ".tar.gz");

                    final CommandLine commandLine;
                    if (this.runAsUserEnabled) {
                        commandLine = new CommandLine("sudo");
                        commandLine.addArgument("tar");
                    } else {
                        commandLine = new CommandLine("tar");
                    }
                    commandLine
                        .addArgument("-c")
                        .addArgument("-z")
                        .addArgument("-f")
                        .addArgument(localArchiveFile.getCanonicalPath())
                        .addArgument("--exclude")
                        .addArgument(localArchiveFile.getName())
                        .addArgument("./");

                    this.executor.setWorkingDirectory(jobDir);

                    log.debug("Archive command : {}", commandLine);
                    try {
                        this.executor.execute(commandLine);
                    } catch (Throwable t) {
                        log.warn("Failed to created archive of job files for job: {}", jobId, t);
                        incrementErrorCounter("JOB_ARCHIVAL_FAILURE", t);
                        throw t;
                    }

                    // Upload the tar file to remote location
                    this.genieFileTransferService.putFile(localArchiveFile.getCanonicalPath(), archiveLocation.get());

                    // At this point the archive file is successfully uploaded to archive location specified in the job.
                    // Now we can delete it from local disk to save space if enabled.
                    if (this.deleteArchiveFile) {
                        log.debug("Deleting archive file");
                        try {
                            if (this.runAsUserEnabled) {
                                final CommandLine deleteCommand = new CommandLine("sudo")
                                    .addArgument("rm")
                                    .addArgument("-f")
                                    .addArgument(localArchiveFile.getCanonicalPath());

                                this.executor.setWorkingDirectory(jobDir);
                                log.debug("Delete command: {}", deleteCommand);
                                this.executor.execute(deleteCommand);
                            } else if (!localArchiveFile.delete()) {
                                log.error("Failed to delete archive file for job: {}", jobId);
                                incrementErrorCounter("JOB_ARCHIVE_DELETION_FAILURE");
                            }
                        } catch (final Exception e) {
                            log.error("Failed to delete archive file for job: {}", jobId, e);
                            incrementErrorCounter("JOB_ARCHIVE_DELETION_FAILURE", e);
                        }
                    }
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * Sends an email when the job is completed. Returns true if an email has been sent.
     *
     * @param jobId The job id.
     * @throws GenieException If there is any problem.
     */
    private boolean sendEmail(final String jobId) throws GenieException {
        final JobRequest jobRequest = this.jobSearchService.getJobRequest(jobId);
        boolean result = false;
        final Optional<String> email = jobRequest.getEmail();

        if (email.isPresent() && !Strings.isNullOrEmpty(email.get())) {
            log.debug("Got a job finished event. Sending email: {}", email.get());
            final JobStatus status = this.jobSearchService.getJobStatus(jobId);

            final StringBuilder subject = new StringBuilder()
                .append("Genie Job Finished. Id: [")
                .append(jobId)
                .append("], Name: [")
                .append(jobRequest.getName())
                .append("], Status: [")
                .append(status)
                .append("].");

            final StringBuilder body = new StringBuilder()
                .append("Id: [")
                .append(jobId)
                .append("]\n")
                .append("Name: [")
                .append(jobRequest.getName())
                .append("]\n")
                .append("Status: [")
                .append(status)
                .append("]\n")
                .append("User: [")
                .append(jobRequest.getUser())
                .append("]\n")
                .append("Tags: ")
                .append(jobRequest.getTags())
                .append("\n");
            jobRequest
                .getDescription()
                .ifPresent(
                    description ->
                        body.append("[")
                            .append(description)
                            .append("]")
                );

            try {
                this.mailServiceImpl.sendEmail(
                    email.get(),
                    subject.toString(),
                    body.toString()
                );
            } catch (Throwable t) {
                incrementErrorCounter("JOB_EMAIL_FAILURE", t);
                throw t;
            }
            result = true;
        }
        return result;
    }

    private void incrementErrorCounter(final String errorTagValue, final Throwable throwable) {
        this.incrementErrorCounter(
            ImmutableSet.of(
                Tag.of(ERROR_SOURCE_TAG, errorTagValue),
                Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, throwable.getClass().getCanonicalName())
            )
        );
    }

    private void incrementErrorCounter(final String errorTagValue) {
        this.incrementErrorCounter(ImmutableSet.of(Tag.of(ERROR_SOURCE_TAG, errorTagValue)));
    }

    private void incrementErrorCounter(final Set<Tag> tags) {
        this.registry.counter(JOB_COMPLETION_ERROR_COUNTER_NAME, tags).increment();
    }
}
