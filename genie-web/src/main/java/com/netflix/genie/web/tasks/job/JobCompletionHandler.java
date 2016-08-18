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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.JobFinishedReason;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobDoneFile;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.MailService;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A class that has the methods to perform various tasks when a job completes.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
@Component
public class JobCompletionHandler {

    private static final String STATUS_TAG = "status";
    private static final String ERROR_TAG = "error";

    private final JobPersistenceService jobPersistenceService;
    private final JobSearchService jobSearchService;
    private final GenieFileTransferService genieFileTransferService;
    private final File baseWorkingDir;
    private final MailService mailServiceImpl;
    private final Executor executor;
    private final boolean deleteArchiveFile;
    private final boolean deleteDependencies;
    private final boolean isRunAsUserEnabled;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Metrics
    private final Registry registry;
    private final Id jobCompletionId;
    private final Counter emailSuccessRate;
    private final Counter emailFailureRate;
    private final Counter archivalFailureRate;
    private final Counter doneFileProcessingFailureRate;
    private final Counter finalStatusUpdateFailureRate;
    private final Counter processGroupCleanupFailureRate;
    private final Counter archiveFileDeletionFailure;
    private final Counter deleteDependenciesFailure;

    /**
     * Constructor.
     *
     * @param jobSearchService         An implementation of the job search service.
     * @param jobPersistenceService    An implementation of the job persistence service.
     * @param genieFileTransferService An implementation of the Genie File Transfer service.
     * @param genieWorkingDir          The working directory where all job directories are created.
     * @param mailServiceImpl          An implementation of the mail service.
     * @param registry                 The metrics registry to use
     * @param deleteArchiveFile        Flag that determines if the job archive file will be deleted after upload.
     * @param deleteDependencies       Flag that determines if the job dependencies will be deleted after job is done.
     * @param isRunAsUserEnabled       Flag that decides where jobs are run as user themselves or genie user.
     * @throws GenieException if there is a problem
     */
    @Autowired
    public JobCompletionHandler(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final GenieFileTransferService genieFileTransferService,
        final Resource genieWorkingDir,
        final MailService mailServiceImpl,
        final Registry registry,
        @Value("${genie.jobs.cleanup.deleteArchiveFile.enabled:true}")
        final boolean deleteArchiveFile,
        @Value("${genie.jobs.cleanup.deleteDependencies.enabled:true}")
        final boolean deleteDependencies,
        @Value("${genie.jobs.runAsUser.enabled:false}")
        final boolean isRunAsUserEnabled
    ) throws GenieException {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.genieFileTransferService = genieFileTransferService;
        this.mailServiceImpl = mailServiceImpl;
        this.deleteArchiveFile = deleteArchiveFile;
        this.deleteDependencies = deleteDependencies;
        this.isRunAsUserEnabled = isRunAsUserEnabled;

        this.executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(null, null));

        try {
            this.baseWorkingDir = genieWorkingDir.getFile();
        } catch (final IOException gse) {
            throw new GenieServerException("Could not load the base path from resource");
        }

        // Set up the metrics
        this.registry = registry;
        this.jobCompletionId = registry.createId("genie.jobs.completion.timer");
        this.emailSuccessRate = registry.counter("genie.jobs.email.success.rate");
        this.emailFailureRate = registry.counter("genie.jobs.email.failure.rate");
        this.archivalFailureRate = registry.counter("genie.jobs.archivalFailure.rate");
        this.doneFileProcessingFailureRate = registry.counter("genie.jobs.doneFileProcessingFailure.rate");
        this.finalStatusUpdateFailureRate = registry.counter("genie.jobs.finalStatusUpdateFailure.rate");
        this.processGroupCleanupFailureRate = registry.counter("genie.jobs.processGroupCleanupFailure.rate");
        this.archiveFileDeletionFailure = registry.counter("genie.jobs.archiveFileDeletionFailure.rate");
        this.deleteDependenciesFailure = registry.counter("genie.jobs.deleteDependenciesFailure.rate");
    }

    /**
     * Event listener for when a job is completed. Updates the status of the job.
     *
     * @param event The Spring Boot application ready event to startup on
     * @throws GenieException If there is any problem
     */
    @EventListener
    public void handleJobCompletion(final JobFinishedEvent event) throws GenieException {
        final long start = System.nanoTime();
        Id timerId = null;
        try {
            final String jobId = event.getId();
            // TODO: What if there is an exception thrown here? Everything is missed
            final Job job = this.jobSearchService.getJob(jobId);
            final JobStatus status = job.getStatus();

            // Make sure the job isn't already done before doing something
            if (status != JobStatus.FAILED
                && status != JobStatus.INVALID
                && status != JobStatus.KILLED
                && status != JobStatus.SUCCEEDED) {
                // Now we know this job should be marked in one of the finished states
                if (status == JobStatus.INIT) {
                    try {
                        switch (event.getReason()) {
                            case KILLED:
                                this.jobPersistenceService.updateJobStatus(
                                    jobId, JobStatus.KILLED, event.getMessage()
                                );
                                timerId = this.jobCompletionId.withTag(STATUS_TAG, JobStatus.KILLED.toString());
                                break;
                            case INVALID:
                                this.jobPersistenceService.updateJobStatus(
                                    jobId, JobStatus.INVALID, event.getMessage()
                                );
                                timerId = this.jobCompletionId.withTag(STATUS_TAG, JobStatus.INVALID.toString());
                                break;
                            case FAILED_TO_INIT:
                                this.jobPersistenceService.updateJobStatus(
                                    jobId, JobStatus.FAILED, event.getMessage()
                                );
                                timerId = this.jobCompletionId.withTag(STATUS_TAG, JobStatus.FAILED.toString());
                                break;
                            case PROCESS_COMPLETED:
                                this.jobPersistenceService.updateJobStatus(
                                    jobId, JobStatus.SUCCEEDED, event.getMessage()
                                );
                                timerId = this.jobCompletionId.withTag(STATUS_TAG, JobStatus.SUCCEEDED.toString());
                                break;
                            case SYSTEM_CRASH:
                                this.jobPersistenceService.updateJobStatus(
                                    jobId, JobStatus.FAILED, event.getMessage()
                                );
                                timerId = this.jobCompletionId.withTag(STATUS_TAG, JobStatus.FAILED.toString());
                                break;
                            default:
                                log.error("Unknown case: " + event.getReason());
                                this.finalStatusUpdateFailureRate.increment();
                                timerId = this.jobCompletionId
                                    .withTag(STATUS_TAG, "exception")
                                    .withTag(ERROR_TAG, event.getReason().toString());
                                break;
                        }
                    } catch (final GenieException ge) {
                        this.finalStatusUpdateFailureRate.increment();
                    }
                } else if (status == JobStatus.RUNNING) {
                    if (event.getReason() != JobFinishedReason.SYSTEM_CRASH) {
                        timerId
                            = this.jobCompletionId.withTag(STATUS_TAG, this.updateFinalStatusForJob(jobId).toString());
                        this.cleanupProcesses(jobId);
                    } else {
                        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.FAILED, event.getMessage());
                        timerId = this.jobCompletionId.withTag(STATUS_TAG, JobStatus.FAILED.toString());
                    }
                }

                // Things that should be done either way
                this.processJobDir(jobId);
                this.sendEmail(jobId);
            }
        } finally {
            if (timerId == null) {
                timerId = this.jobCompletionId.withTag(STATUS_TAG, "error").withTag(ERROR_TAG, "Unknown");
            }
            this.registry.timer(timerId).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * An external fail-safe mechanism to clean up processes left behind by the run.sh after the
     * job is killed or failed.
     *
     * @param jobId The id of the job to cleanup processes for.
     */
    private void cleanupProcesses(final String jobId) {
        try {
            final Optional<Integer> pid = this.jobSearchService.getJobExecution(jobId).getProcessId();
            if (pid.isPresent()) {
                try {
                    final CommandLine commandLine = new CommandLine(JobConstants.UNIX_PKILL_COMMAND);
                    commandLine.addArgument(JobConstants.getKillFlag());
                    commandLine.addArgument(Integer.toString(pid.get()));
                    this.executor.execute(commandLine);

                    // The process group should not exist and the above code should always throw and exception.
                    // If it does not then the bash script is not cleaning up stuff well during kills or the script is
                    // done but child processes are still remaining. This metric tracks all that.
                    this.processGroupCleanupFailureRate.increment();
                } catch (final Exception e) {
                    log.debug("Received expected exception. Ignoring.");
                }
            }
        } catch (final GenieException ge) {
            log.error("Unable to get job execution so unable to cleanup process for job " + jobId, ge);
            this.processGroupCleanupFailureRate.increment();
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
        try {
            log.debug("Updating the status of the job.");

            try {
                final File jobDir = new File(this.baseWorkingDir, id);
                final JobDoneFile jobDoneFile = this.objectMapper.readValue(
                    new File(jobDir, "genie/genie.done"),
                    JobDoneFile.class
                );
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
                            "Job was killed.",
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
                            "Job finished successfully.",
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
                            "Job failed.",
                            stdOutSize,
                            stdErrSize
                        );
                        finalStatus = JobStatus.FAILED;
                        break;
                }

                return finalStatus;
            } catch (final IOException ioe) {
                this.doneFileProcessingFailureRate.increment();
                // The run.sh should theoretically ALWAYS generate a done file so we should never hit this code.
                // But if we do handle it generate a metric for it which we can track
                log.error("Could not load the done file for job {}. Marking it as failed.", id);
                this.jobPersistenceService.updateJobStatus(
                    id,
                    JobStatus.FAILED,
                    "Genie could not load done file."
                );
                return JobStatus.FAILED;
            }
        } catch (final Exception e) {
            log.error("Could not update the exit code and status for job: {}", id, e);
            this.finalStatusUpdateFailureRate.increment();
            throw e;
        }
    }

    /**
     * Delete the application dependencies off disk to save space.
     *
     * @param jobId  The ID of the job to delete dependencies for
     * @param jobDir The job working directory
     */
    private void deleteApplicationDependencies(final String jobId, final File jobDir) {
        log.debug("Deleting dependencies as its enabled.");
        if (jobDir.exists()) {
            try {
                final List<String> appIds = this.jobSearchService
                    .getJobApplications(jobId)
                    .stream()
                    .map(Application::getId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

                for (final String appId : appIds) {
                    final File appDependencyDir = new File(
                        jobDir,
                        JobConstants.GENIE_PATH_VAR
                            + JobConstants.FILE_PATH_DELIMITER
                            + JobConstants.APPLICATION_PATH_VAR
                            + JobConstants.FILE_PATH_DELIMITER
                            + appId
                            + JobConstants.FILE_PATH_DELIMITER
                            + JobConstants.DEPENDENCY_FILE_PATH_PREFIX
                    );

                    if (appDependencyDir.exists()) {
                        if (this.isRunAsUserEnabled) {
                            final CommandLine deleteCommand = new CommandLine("sudo");
                            deleteCommand.addArgument("rm");
                            deleteCommand.addArgument("-rf");
                            deleteCommand.addArgument(appDependencyDir.getCanonicalPath());
                            log.debug("Delete command is {}", deleteCommand.toString());
                            this.executor.execute(deleteCommand);
                        } else {
                            FileUtils.deleteDirectory(appDependencyDir);
                        }
                    }
                }
            } catch (final Exception e) {
                log.error("Could not delete job dependencies after completion for job: {} due to error {}",
                    jobId, e);
                this.deleteDependenciesFailure.increment();
            }
        }
    }

    /**
     * Uploads the job directory to the archive location.
     *
     * @param jobId The job id.
     * @throws GenieException if there is any problem
     */
    private void processJobDir(final String jobId) throws GenieException {
        log.debug("Got a job finished event. Will process job directory.");

        final Job job = this.jobSearchService.getJob(jobId);
        final File jobDir = new File(this.baseWorkingDir, jobId);

        if (jobDir.exists()) {
            if (this.deleteDependencies) {
                this.deleteApplicationDependencies(jobId, jobDir);
            }

            try {
                // If archive location is provided create a tar and upload it
                final Optional<String> archiveLocation = job.getArchiveLocation();
                if (archiveLocation.isPresent() && StringUtils.isNotBlank(archiveLocation.get())) {
                    log.debug("Archiving job directory");
                    // Create the tar file
                    final File localArchiveFile = new File(jobDir, "genie/logs/" + jobId + ".tar.gz");

                    final CommandLine commandLine = new CommandLine("sudo");
                    commandLine.addArgument("tar");
                    commandLine.addArgument("-c");
                    commandLine.addArgument("-z");
                    commandLine.addArgument("-f");
                    commandLine.addArgument(localArchiveFile.getCanonicalPath());
                    commandLine.addArgument("./");

                    this.executor.setWorkingDirectory(jobDir);

                    log.debug("Archive command : {}", commandLine.toString());
                    this.executor.execute(commandLine);

                    // Upload the tar file to remote location
                    this.genieFileTransferService.putFile(localArchiveFile.getCanonicalPath(), archiveLocation.get());

                    // At this point the archive file is successfully uploaded to archive location specified in the job.
                    // Now we can delete it from local disk to save space if enabled.
                    if (this.deleteArchiveFile) {
                        log.debug("Deleting archive file");
                        try {
                            if (!localArchiveFile.delete()) {
                                log.error("Failed to delete archive file for job: {}", jobId);
                                this.archiveFileDeletionFailure.increment();
                            }
                        } catch (final Exception e) {
                            log.error("Failed to delete archive file for job: {}", jobId, e);
                            this.archiveFileDeletionFailure.increment();
                        }
                    }
                }
            } catch (final Exception e) {
                log.error("Could not archive directory for job: {}", jobId, e);
                this.archivalFailureRate.increment();
            }
        }
    }

    /**
     * Sends an email when the job is completed.
     *
     * @param jobId The job id.
     * @throws GenieException If there is any problem.
     */
    private void sendEmail(final String jobId) throws GenieException {
        try {
            log.debug("Got a job finished event. Sending email.");
            final JobRequest jobRequest = this.jobSearchService.getJobRequest(jobId);

            final Optional<String> email = jobRequest.getEmail();
            if (email.isPresent() && StringUtils.isNotBlank(email.get())) {
                final JobStatus status = this.jobSearchService.getJobStatus(jobId);
                this.mailServiceImpl.sendEmail(
                    email.get(),
                    "Genie Job" + jobId,
                    "Job with id [" + jobId + "] finished with status " + status
                );
                this.emailSuccessRate.increment();
            }
        } catch (final Exception e) {
            log.error("Could not send email for job: {}", jobId, e);
            this.emailFailureRate.increment();
        }
    }
}
