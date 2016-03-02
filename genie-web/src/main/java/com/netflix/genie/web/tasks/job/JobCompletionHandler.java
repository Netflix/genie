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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.jobs.JobDoneFile;
import com.netflix.genie.core.services.JobPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
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

    private JobPersistenceService jobPersistenceService;
    private String baseWorkingDir;

    /**
     * Constructor.
     *
     * @param jps An implementation of the job persistence service
     * @param genieWorkingDir The working directory where all job directories are created
     */
    @Autowired
    public JobCompletionHandler(
        final JobPersistenceService jps,
        @Value("${genie.jobs.dir.location:/mnt/tomcat/genie-jobs}")
        final String genieWorkingDir
        ) {
        this.jobPersistenceService = jps;
        this.baseWorkingDir = genieWorkingDir;
    }
    /**
     * Event listener for when a job is completed. Updates the status of the job.
     *
     * @param event The Spring Boot application ready event to startup on
     *
     * @throws GenieException If there is any problem
     */
    @EventListener
    public void updateStatus(
        final JobFinishedEvent event
    ) throws GenieException {
        log.debug("Got a job finished event. Will update the status of the job.");
        final String jobId = event.getJobExecution().getId();
        // read the done file and get exit code to decide status
        final ObjectMapper objectMapper = new ObjectMapper();
        // Move logic to fetch file name in some function somewhere

        JobDoneFile jobDoneFile = null;

        try {
            // TODO add retries and move out logic to get done file somewhere else.
            jobDoneFile = objectMapper.readValue(
                new File(baseWorkingDir + "/" + jobId + "/genie/genie.done"), JobDoneFile.class);

        } catch (IOException ioe) {
            log.error("Could not load the done file for job {}. Marking it as failed.", jobId);
            this.jobPersistenceService.updateJobStatus(
                jobId,
                JobStatus.FAILED,
                "Genie could not load done file."
            );
            return;
        }
        final int exitCode = jobDoneFile.getExitCode();

        // Todo create an enumeration of exit codes?
        switch (exitCode) {
            case 0:
                this.jobPersistenceService.updateJobStatus(
                    jobId,
                    JobStatus.SUCCEEDED,
                    "Job finished successfully"
                );
                break;
            case 999:
                this.jobPersistenceService.updateJobStatus(
                    jobId,
                    JobStatus.KILLED,
                    "Job was Killed"
                );
                break;
            default:
                this.jobPersistenceService.updateJobStatus(
                    jobId,
                    JobStatus.FAILED,
                    "Job Failed with exit code " + exitCode
                );
        }
    }

    /**
     * Event listener for when a job is completed. Uploads the job directory to the archive location
     *
     * @param event The Spring Boot application ready event to startup on
     */
    @EventListener
    public void archivedJobDir(final JobFinishedEvent event) {
        log.debug("Got a job finished event. Will archive job directory if enabled.");
    }

    /**
     * Event listener for when a job is completed. Sends an email
     *
     * @param event The Spring Boot application ready event to startup on
     */
    @EventListener
    public void sendEmail(final JobFinishedEvent event) {
        log.debug("Got a job finished event. Will send an email if enabled.");
    }
}
