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
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobDoneFile;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.MailService;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that has the methods to perform various tasks when a job completes.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
@Component
public class JobCompletionHandler {

    private final JobPersistenceService jobPersistenceService;
    private final JobSearchService jobSearchService;
    private final GenieFileTransferService genieFileTransferService;
    private final String baseWorkingDir;
    private final MailService mailServiceImpl;
    private final Executor executor;
    private final boolean deleteArchiveFile;
    private final boolean deleteDependencies;
    private final boolean isRunAsUserEnabled;

    // Metrics
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
     *
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
            this.baseWorkingDir = genieWorkingDir.getFile().getCanonicalPath();
        } catch (IOException gse) {
            throw new GenieServerException("Could not load the base path from resource");
        }

        // Set up the metrics
        this.emailFailureRate = registry.counter("genie.jobs.emailFailure.rate");
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
    public void handleJobCompletion(
        final JobFinishedEvent event
    ) throws GenieException {

        final String jobId = event.getJobExecution().getId();

        updateFinalStatusForJob(jobId);
        cleanupProcesses(event.getJobExecution().getProcessId());
        processJobDir(jobId);
        sendEmail(jobId);
    }

    /**
     * An external fail-safe mechanism to clean up processes left behind by the run.sh after the
     * job is killed or failed.
     *
     * @param pid The process id.
     * @throws GenieException
     */
    private void cleanupProcesses(
        final int pid
    ) throws GenieException {
        try {
            final CommandLine commandLine = new CommandLine(JobConstants.UNIX_PKILL_COMMAND);
            commandLine.addArgument(JobConstants.getKillFlag());
            commandLine.addArgument(Integer.toString(pid));
            executor.execute(commandLine);

            // The process group should not exist and the above code should always throw and exception. If it does
            // not then the bash script is not cleaning up stuff well during kills or the script is done but
            // child processes are still remaining. This metric tracks all that.
            this.processGroupCleanupFailureRate.increment();
        } catch (Exception e) {
            log.debug("Received expected exception. Ignoring.");
        }
    }

    /**
     * Updates the status of the job.
     *
     * @param jobId The job id.
     * @throws GenieException If there is any problem
     */
    public void updateFinalStatusForJob(
        final String jobId
    ) throws GenieException {
        try {
            log.debug("Updating the status of the job.");

            // read the done file and get exit code to decide status
            final ObjectMapper objectMapper = new ObjectMapper();

            try {
                final JobDoneFile jobDoneFile = objectMapper
                    .readValue(new File(baseWorkingDir + "/" + jobId + "/genie/genie.done"), JobDoneFile.class);
                final int exitCode = jobDoneFile.getExitCode();

                // This method internally also updates the status according to the exit code.
                this.jobPersistenceService.setExitCode(jobId, exitCode);
            } catch (final IOException ioe) {
                this.doneFileProcessingFailureRate.increment();
                // The run.sh should theoretically ALWAYS generate a done file so we should never hit this code.
                // But if we do handle it generate a metric for it which we can track
                log.error("Could not load the done file for job {}. Marking it as failed.", jobId);
                this.jobPersistenceService.updateJobStatus(
                    jobId,
                    JobStatus.FAILED,
                    "Genie could not load done file."
                );
            }
        } catch (Exception e) {
            log.error("Could not update the exit code and status for job: {}", jobId, e);
            this.finalStatusUpdateFailureRate.increment();
        }
    }

    /**
     * Uploads the job directory to the archive location.
     *
     * @param jobId The job id.
     * @throws GenieException if there is any problem
     */
    public void processJobDir(
        final String jobId
    ) throws GenieException {
        log.debug("Got a job finished event. Will process job directory.");

        final Job job = this.jobSearchService.getJob(jobId);
        final String jobWorkingDir = this.baseWorkingDir + JobConstants.FILE_PATH_DELIMITER + jobId;

        if (deleteDependencies) {
            log.debug("Deleting dependencies as its enabled.");
            try {
                final List<String> appIds = jobSearchService
                    .getJobApplications(jobId)
                    .stream()
                    .map(Application::getId)
                    .collect(Collectors.toList());

                for (String appId : appIds) {
                    final String applicationDependencyFolder = jobWorkingDir
                        + JobConstants.FILE_PATH_DELIMITER
                        + JobConstants.GENIE_PATH_VAR
                        + JobConstants.FILE_PATH_DELIMITER
                        + JobConstants.APPLICATION_PATH_VAR
                        + JobConstants.FILE_PATH_DELIMITER
                        + appId
                        + JobConstants.FILE_PATH_DELIMITER
                        + JobConstants.DEPENDENCY_FILE_PATH_PREFIX;

                    final CommandLine  deleteCommand;
                    if (isRunAsUserEnabled) {
                        deleteCommand = new CommandLine("sudo");
                        deleteCommand.addArgument("rm");
                    } else {
                        deleteCommand = new CommandLine("rm");
                    }

                    deleteCommand.addArgument("-rf");
                    deleteCommand.addArgument(applicationDependencyFolder);

                    log.debug("Delete command is {}", deleteCommand.toString());
                    executor.execute(deleteCommand);
                }
            } catch (Exception e) {
                log.error("Could not delete job dependencies after completion for job: {} due to error {}",
                    jobId, e);
                this.deleteDependenciesFailure.increment();
            }
        }

        try {
            // If archive location is provided create a tar and upload it
            if (StringUtils.isNotBlank(job.getArchiveLocation())) {

                log.debug("Archiving job directory");
                // Create the tar file
                final String localArchiveFile = jobWorkingDir
                    + JobConstants.FILE_PATH_DELIMITER
                    + "genie/logs/"
                    + jobId
                    + ".tar.gz";

                final CommandLine commandLine = new CommandLine("sudo");
                commandLine.addArgument("tar");
                commandLine.addArgument("-c");
                commandLine.addArgument("-z");
                commandLine.addArgument("-f");
                commandLine.addArgument(localArchiveFile);
                commandLine.addArgument("./");

                executor.setWorkingDirectory(new File(jobWorkingDir));

                log.debug("Archive command : {}", commandLine.toString());
                executor.execute(commandLine);

                // Upload the tar file to remote location
                this.genieFileTransferService.putFile(localArchiveFile, job.getArchiveLocation());

                // At this point the archive file is successfully uploaded to archvie location specified in the job.
                // Now we can delete it from local disk to save space if enabled.
                if (deleteArchiveFile) {
                    log.debug("Deleting archive file");
                    try {
                        new File(localArchiveFile).delete();
                    } catch (Exception e) {
                        log.error("Failed to delete archive file for job: {}", jobId, e);
                        this.archiveFileDeletionFailure.increment();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Could not archive directory for job: {}", jobId, e);
            this.archivalFailureRate.increment();
        }
    }

    /**
     * Sends an email when the job is completed.
     *
     * @param jobId The job id.
     * @throws GenieException If there is any problem.
     */
    public void sendEmail(
        final String jobId
    ) throws GenieException {
        try {
            log.debug("Got a job finished event. Sending email.");

            final JobRequest jobRequest = this.jobSearchService.getJobRequest(jobId);
            final Job job = this.jobSearchService.getJob(jobId);

            if (StringUtils.isNotBlank(jobRequest.getEmail())) {
                final String message = new StringBuilder()
                    .append("Job with id [")
                    .append(jobId).append("] finished with status ")
                    .append(job.getStatus())
                    .toString();

                this.mailServiceImpl.sendEmail(
                    jobRequest.getEmail(),
                    message,
                    message
                );
            }
        } catch (Exception e) {
            log.error("Could not send email for job: {}", jobId, e);
            this.emailFailureRate.increment();
        }
    }
}
