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
package com.netflix.genie.core.services;

import com.netflix.genie.common.dto.Job;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobExecutionEnvironment;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;

import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;

/**
 * Interfaces for providing persistence functions for jobs other than search.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface JobPersistenceService {

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    Job getJob(
            @NotBlank(message = "No id entered. Unable to get job.")
            final String id
    ) throws GenieException;

    /**
     * Save the job object in the data store.
     *
     * @param job the Job object to create
     * @throws GenieException if there is an error
     */
    void createJob(
            @NotNull(message = "Job is null so cannot be saved")
            final Job job
    ) throws GenieException;

    /**
     * Update the execution environment for the job.
     *
     * @param jee The job execution environment information for a job
     * @throws GenieException if there is an error
     */
    void addJobExecutionEnvironmentToJob(
            final JobExecutionEnvironment jee
    ) throws GenieException;

    /**
     * Return the Job Request Entity for the  id provided.
     *
     * @param id The id of the jobRequest to return.
     * @return The job request details or null if not found
     * @throws GenieException if there is an error
     */
    JobRequest getJobRequest(
            final String id
    ) throws GenieException;

    /**
     * Save the jobRequest object in the data store.
     *
     * @param jobRequest the Job object to save
     *
     * @return The id of the job Request that was created
     * @throws GenieException if there is an error
     */
    String createJobRequest(
            @NotNull(message = "Job Request is null so cannot be saved")
            final JobRequest jobRequest
    ) throws GenieException;

    /**
     * Return the Job Entity for the job id provided.
     *
     * @param id The id of the job to return.
     * @return Job Execution details or null if not found
     * @throws GenieException if there is an error
     */
    JobExecution getJobExecution(
            final String id
    ) throws GenieException;

    /**
     * Save the jobExecution object in the data store.
     *
     * @param jobExecution the Job object to save
     * @throws GenieException if there is an error
     */
    void createJobExecution(
            @NotNull(message = "Job Request is null so cannot be saved")
            final JobExecution jobExecution
    ) throws GenieException;
}
