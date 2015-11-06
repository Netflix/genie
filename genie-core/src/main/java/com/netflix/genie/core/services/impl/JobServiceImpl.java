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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;

import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobService;

import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.List;


/**
 * Implementation of the JobService apis.
 *
 * @author amsharma
 */
@Service
public class JobServiceImpl implements JobService {

    private static final Logger LOG = LoggerFactory.getLogger(JobServiceImpl.class);

    @Autowired
    @Qualifier("search")
    private final List<JobPersistenceService> searchPriorityOrderPersistenceList;

  //  private final List<JobPersistenceService> savePriorityOrderPersistenceList;

    /**
     *   Constructor.
     *
     */
    public JobServiceImpl(
//            @Qualifier("search")
//            final List<JobPersistenceService> searchPriorityOrderPersistenceList
////            @Qualifier("persistenceSavePriorityOrder")
////            final List<JobPersistenceService> savePriorityOrderPersistenceList
    ) {
        this.searchPriorityOrderPersistenceList = null;
      //  this.savePriorityOrderPersistenceList = savePriorityOrderPersistenceList;
    }
    /**
     * Takes in a Job Request object and does necessary preparation for execution.
     *
     * @param jobRequest of job to kill
     * @throws GenieException if there is an error
     */
    @Override
    public void runJob(
            @NotNull(message = "No jobRequest provided. Unable to submit job for execution.")
            @Valid
            final JobRequest jobRequest
    ) throws GenieException {
        // do basic validation of the request
        // persist in various storage layers
        // submit the job request to Job submitter interface
    }

    /**
     * Gets the Job object to return to user given the id.
     *
     * @param jobId of job to retrieve
     * @return job object
     * @throws GenieException if there is an error
     */
    @Override
    public Job getJob(
            @NotBlank(message = "No job id provided. Unable to retrieve job.")
            final String jobId) throws GenieException {


        searchPriorityOrderPersistenceList.forEach(jpsimpl -> LOG.info(jpsimpl.getClass().toString()));
        return null;
    }

    /**
     * Get list of jobs for given filter criteria.
     *
     * @param id          id for job
     * @param jobName     name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName    user who submitted job
     * @param statuses    statuses of job
     * @param tags        tags for the job
     * @param clusterName name of cluster for job
     * @param clusterId   id of cluster for job
     * @param commandName name of the command run in the job
     * @param commandId   id of the command run in the job
     * @param page        Page information of jobs to get
     * @return All jobs which match the criteria
     */
    @Override
    public Page<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final Pageable page) {
        return null;
    }

    /**
     * Takes in a id of the job to kill.
     *
     * @param jobId id of the job to kill
     * @throws GenieException if there is an error
     */
    @Override
    public void killJob(
            @NotBlank(message = "No job id provided. Unable to retrieve job.")
            final String jobId
    ) throws GenieException {

    }
}
