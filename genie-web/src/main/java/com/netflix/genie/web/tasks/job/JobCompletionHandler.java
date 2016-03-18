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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

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

    /**
     * Constructor.
     *
     * @param jobSearchService         An implementation of the job search service
     * @param jobPersistenceService    An implementation of the job persistence service
     * @param genieFileTransferService An implementation of the Genie File Transfer service
     * @param genieWorkingDir          The working directory where all job directories are created
     * @param mailServiceImpl          An implementation of the mail service
     * @throws GenieException if there is a problem
     */
    @Autowired
    public JobCompletionHandler(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final GenieFileTransferService genieFileTransferService,
        final Resource genieWorkingDir,
        final MailService mailServiceImpl
    ) throws GenieException {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.genieFileTransferService = genieFileTransferService;
        this.mailServiceImpl = mailServiceImpl;

        try {
            this.baseWorkingDir = genieWorkingDir.getFile().getCanonicalPath();
        } catch (IOException gse) {
            throw new GenieServerException("Could not load the base path from resource");
        }
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

        updateExitCode(jobId);
        archivedJobDir(jobId);
        sendEmail(jobId);
    }

    /**
     * Updates the status of the job.
     *
     * @param jobId The job id.
     * @throws GenieException If there is any problem
     */
    public void updateExitCode(
        final String jobId
    ) throws GenieException {
        log.debug("Got a job finished event. Will update the status of the job.");

        // read the done file and get exit code to decide status
        final ObjectMapper objectMapper = new ObjectMapper();

        try {
            final JobDoneFile jobDoneFile = objectMapper
                .readValue(new File(baseWorkingDir + "/" + jobId + "/genie/genie.done"), JobDoneFile.class);
            final int exitCode = jobDoneFile.getExitCode();
            this.jobPersistenceService.setExitCode(jobId, exitCode);
        } catch (final IOException ioe) {
            log.error("Could not load the done file for job {}. Marking it as failed.", jobId);
            this.jobPersistenceService.updateJobStatus(
                jobId,
                JobStatus.FAILED,
                "Genie could not load done file."
            );
        }
    }

    /**
     * Uploads the job directory to the archive location.
     *
     * @param jobId The job id.
     * @throws GenieException if there is any problem
     */
    public void archivedJobDir(
        final String jobId
    ) throws GenieException {
        log.debug("Got a job finished event. Will archive job directory if enabled.");

        final Job job = this.jobSearchService.getJob(jobId);

        if (StringUtils.isNotBlank(job.getArchiveLocation())) {

            // Create the tar file exluding the run.sh file and everything under the genie directory
            final String jobWorkingDir = this.baseWorkingDir + JobConstants.FILE_PATH_DELIMITER + jobId;
            final String localArchiveFile = jobWorkingDir + JobConstants.FILE_PATH_DELIMITER + jobId + ".tar.gz";

            final CommandLine commandLine = new CommandLine("tar");
            commandLine.addArgument("--exclude=genie");
            commandLine.addArgument("--exclude=run.sh");
            commandLine.addArgument("-c");
            commandLine.addArgument("-z");
            commandLine.addArgument("-f");
            commandLine.addArgument(localArchiveFile);
            commandLine.addArgument("./");

            final Executor executor = new DefaultExecutor();
            executor.setWorkingDirectory(new File(jobWorkingDir));
            executor.setStreamHandler(new PumpStreamHandler(null, null));

            try {
                executor.execute(commandLine);
                // TODO whats the point of this exception? who catches it?
            } catch (IOException ioe) {
                throw new GenieServerException("Could not tar the output directory of job " + jobId + ioe);
            }

            // Upload the tar file to remote location
            this.genieFileTransferService.putFile(localArchiveFile, job.getArchiveLocation());
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
        log.debug("Got a job finished event. Sending email.");

        final JobRequest jobRequest = this.jobSearchService.getJobRequest(jobId);
        final Job job = this.jobSearchService.getJob(jobId);

        if (org.apache.commons.lang3.StringUtils.isNotBlank(jobRequest.getEmail())) {
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
    }
}
