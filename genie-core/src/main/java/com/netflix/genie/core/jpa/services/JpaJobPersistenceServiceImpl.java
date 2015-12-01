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
package com.netflix.genie.core.jpa.services;

import com.netflix.genie.common.dto.Job;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.constraints.NotNull;
import java.util.UUID;

/**
 * JPA implementation of the job persistence service.
 *
 * @author amsharma
 */
@Service
@Transactional(
        rollbackFor = {
                GenieException.class,
                ConstraintViolationException.class
        }
)
public class JpaJobPersistenceServiceImpl implements JobPersistenceService {

    private static final Logger LOG = LoggerFactory.getLogger(JpaJobPersistenceServiceImpl.class);
    private final JpaJobRepository jobRepo;
    private final JpaJobRequestRepository jobRequestRepo;
    private final JpaJobExecutionRepository jobExecutionRepo;

    /**
     * Default Constructor.
     *
     * @param jobRepo The job repository to use
     * @param jobRequestRepo The job request repository to use
     * @param jobExecutionRepo The jobExecution Repository to use
     */
    @Autowired
    public JpaJobPersistenceServiceImpl(
            final JpaJobRepository jobRepo,
            final JpaJobRequestRepository jobRequestRepo,
            final JpaJobExecutionRepository jobExecutionRepo
    ) {
        this.jobRepo = jobRepo;
        this.jobRequestRepo = jobRequestRepo;
        this.jobExecutionRepo = jobExecutionRepo;
    }
    /**
     {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Job getJob(
            @NotBlank(message = "No id entered. Unable to get job.")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.findJob(id).getDTO();
    }

    /**
     {@inheritDoc}
     */
    @Override
    public void createJob(
            @NotNull(message = "Job is null so cannot be saved")
            final Job job
    ) throws GenieException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with job: " + job.toString());
        }

        // Since a job request object should always exist before the job object is saved
        // the id should never be null
        if (StringUtils.isBlank(job.getId())) {
            throw new GenieConflictException("Cannot create a job with id blank or null");
        }

        if (this.jobRepo.exists(job.getId())) {
            throw new GenieConflictException("A job with id " + job.getId() + " already exists");
        }

        final JobEntity jobEntity = new JobEntity();


        jobEntity.setId(job.getId());
        jobEntity.setName(job.getName());
        jobEntity.setUser(job.getUser());
        jobEntity.setVersion(job.getVersion());
        jobEntity.setArchiveLocation(job.getArchiveLocation());
        jobEntity.setDescription(job.getDescription());
        jobEntity.setStarted(job.getStarted());
        jobEntity.setStatus(job.getStatus());
        jobEntity.setStatusMsg(job.getStatusMsg());

        // TODO where are tags sorted?
        jobEntity.setJobTags(job.getTags());

        // TODO: where are the ones below set .. update method?
        // command_id, command_name, cluster_id, cluster_name, exit_code, finished,
        this.jobRepo.save(jobEntity);
    }

    /**
     {@inheritDoc}
     */
    @Override
    public String createJobRequest(
            @NotNull(message = "Job Request is null so cannot be saved")
            final JobRequest jobRequest
    ) throws GenieException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with jobRequest: " + jobRequest.toString());
        }

        if (jobRequest.getId() != null && this.jobRequestRepo.exists(jobRequest.getId())) {
            throw new GenieConflictException("A job with id " + jobRequest.getId() + " already exists");
        }

        final JobRequestEntity jobRequestEntity = new JobRequestEntity();

        jobRequestEntity.setName(jobRequest.getName());
        jobRequestEntity.setUser(jobRequest.getUser());
        jobRequestEntity.setVersion(jobRequest.getVersion());
        jobRequestEntity.setDescription(jobRequest.getDescription());
        jobRequestEntity.setCommandArgs(jobRequest.getCommandArgs());
        jobRequestEntity.setGroup(jobRequest.getGroup());
        jobRequestEntity.setSetupFile(jobRequest.getSetupFile());
        jobRequestEntity.setClusterCriteriasFromList(jobRequest.getClusterCriterias());
        jobRequestEntity.setCommandCriteriaFromSet(jobRequest.getCommandCriteria());

        // TODO convert set to csv
        //jobRequestEntity.setFileDependencies(jobRequest.getFileDependencies());

        jobRequestEntity.setDisableLogArchival(jobRequest.getDisableLogArchival());
        jobRequestEntity.setEmail(jobRequest.getEmail());

        // TODO sort tags?
        jobRequestEntity.setTags(jobRequest.getTags());

        jobRequestEntity.setCpu(jobRequest.getCpu());
        jobRequestEntity.setMemory(jobRequest.getMemory());

        // TODO client host should be part of jobRequest?
        //jobRequestEntity.setClientHost(jobRequest.);

        if (StringUtils.isBlank(jobRequestEntity.getId())) {
            jobRequestEntity.setId(UUID.randomUUID().toString());
        }

        final String id = jobRequestEntity.getId();
        this.jobRequestRepo.save(jobRequestEntity);
        return id;
    }

    /**
     {@inheritDoc}
     */
    @Override
    public void createJobExecution(
            @NotNull(message = "Job Request is null so cannot be saved")
            final JobExecution jobExecution
    ) throws GenieException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with jobExecution: " + jobExecution.toString());
        }

        if (jobExecution.getId() != null && this.jobExecutionRepo.exists(jobExecution.getId())) {
            throw new GenieConflictException("A job with id " + jobExecution.getId() + " already exists");
        }

        final JobExecutionEntity jobExecutionEntity = new JobExecutionEntity();

        jobExecutionEntity.setId(jobExecution.getId());
        jobExecutionEntity.setClusterCriteriaFromSet(jobExecution.getClusterCriteria());
        jobExecutionEntity.setHostName(jobExecution.getHostName());
        jobExecutionEntity.setProcessId(jobExecution.getProcessId());

        this.jobExecutionRepo.save(jobExecutionEntity);
    }

    /**
     * Helper to find an application entity based on ID.
     *
     * @param id The id of the application to find
     * @return The application entity if one is found
     * @throws GenieNotFoundException If no application is found
     */
    private JobEntity findJob(final String id)
            throws GenieNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called");
        }
        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity != null) {
            return jobEntity;
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }
}
