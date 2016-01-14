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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
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
import javax.validation.Valid;
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
    private final JpaClusterRepository clusterRepo;
    private final JpaCommandRepository commandRepo;

    /**
     * Default Constructor.
     *
     * @param jobRepo          The job repository to use
     * @param jobRequestRepo   The job request repository to use
     * @param jobExecutionRepo The jobExecution Repository to use
     * @param clusterRepo      The cluster repository to use
     * @param commandRepo      The command repository to use
     */
    @Autowired
    public JpaJobPersistenceServiceImpl(
        final JpaJobRepository jobRepo,
        final JpaJobRequestRepository jobRequestRepo,
        final JpaJobExecutionRepository jobExecutionRepo,
        final JpaClusterRepository clusterRepo,
        final JpaCommandRepository commandRepo
    ) {
        this.jobRepo = jobRepo;
        this.jobRequestRepo = jobRequestRepo;
        this.jobExecutionRepo = jobExecutionRepo;
        this.clusterRepo = clusterRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(
        @NotBlank(message = "No id entered. Unable to get job.")
        final String id
    ) throws GenieNotFoundException {
        LOG.debug("Called");
        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity != null) {
            return jobEntity.getDTO();
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void createJob(
        @NotNull(message = "Job is null so cannot be saved")
        final Job job
    ) throws GenieException {
        LOG.debug("Called with job: {}", job);

        // Since a job request object should always exist before the job object is saved
        // the id should never be null
        if (StringUtils.isBlank(job.getId())) {
            throw new GenieConflictException("Cannot create a job with id blank or null");
        }

        if (this.jobRepo.exists(job.getId())) {
            throw new GenieConflictException("A job with id " + job.getId() + " already exists");
        }

        final JobRequestEntity jobRequestEntity = jobRequestRepo.findOne(job.getId());

        if (jobRequestEntity == null) {
            throw new GenieNotFoundException("Cannot find the job request for the id of the job specified.");
        }

        final JobEntity jobEntity = new JobEntity();

        jobEntity.setId(job.getId());
        jobEntity.setName(job.getName());
        jobEntity.setUser(job.getUser());
        jobEntity.setVersion(job.getVersion());
        jobEntity.setArchiveLocation(job.getArchiveLocation());
        jobEntity.setDescription(job.getDescription());
        if (job.getStarted() != null) {
            jobEntity.setStarted(job.getStarted());
        }

        jobEntity.setStatus(job.getStatus());
        jobEntity.setStatusMsg(job.getStatusMsg());

        // TODO where are tags sorted?
        jobEntity.setJobTags(job.getTags());

        // TODO: where are the ones below set .. update method?
        // finished,

        jobRequestEntity.setJob(jobEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobStatus(
        @NotBlank(message = "No job id entered. Unable to update.")
        final String id,
        @NotNull (message = "Status cannot be null.")
        final JobStatus jobStatus,
        @NotBlank(message = "Status message cannot be empty.")
        final String statusMsg
    ) throws GenieException {
        if (!this.jobRepo.exists(id)) {
            throw new GenieNotFoundException("No job information entered. Unable to update.");
        }

        LOG.debug("Called to update job with id , status, statusMsg {} {} {}", id, jobStatus, statusMsg);

        final JobEntity jobEntity = this.jobRepo.findOne(id);

        jobEntity.setStatus(jobStatus);
        jobEntity.setStatusMsg(statusMsg);

        this.jobRepo.save(jobEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void addJobExecutionEnvironmentToJob(
        @NotNull(message = "Job Execution environment is null so cannot update")
        final JobExecutionEnvironment jee
    ) throws GenieException {
        LOG.debug("Called");

        if (jee.getJobRequest() == null || jee.getJobRequest().getId() == null) {
            throw new GenieBadRequestException("Cannot update job as it is null");
        }

        final String jobId = jee.getJobRequest().getId();
        ClusterEntity clusterEntity = null;
        CommandEntity commandEntity = null;

        final JobEntity jobEntity = this.jobRepo.findOne(jobId);

        if (jee.getCluster() != null && jee.getCluster().getId() != null) {
            clusterEntity = this.clusterRepo.findOne(jee.getCluster().getId());
            if (clusterEntity == null) {
                throw new GenieNotFoundException("No cluster with id " + jee.getCluster().getId());
            }
        }

        if (jee.getCommand() != null && jee.getCommand().getId() != null) {
            commandEntity = this.commandRepo.findOne(jee.getCommand().getId());
            if (commandEntity == null) {
                throw new GenieNotFoundException("No command with id " + jee.getCommand().getId());
            }
        }

        if (jobEntity != null) {
            jobEntity.setCluster(clusterEntity);
            jobEntity.setClusterName(clusterEntity.getName());
            jobEntity.setCommand(commandEntity);
            jobEntity.setCommandName(commandEntity.getName());

            this.jobRepo.save(jobEntity);

        } else {
            throw new GenieNotFoundException("No job with id " + jobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public JobRequest getJobRequest(
        @NotBlank(message = "No id entered. Unable to get job request.")
        final String id
    ) throws GenieException {
        LOG.debug("Called");
        final JobRequestEntity jobRequestEntity = this.jobRequestRepo.findOne(id);
        if (jobRequestEntity != null) {
            return jobRequestEntity.getDTO();
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequest createJobRequest(
        @NotNull(message = "Job Request is null so cannot be saved")
        final JobRequest jobRequest
    ) throws GenieException {
        LOG.debug("Called with jobRequest: {}", jobRequest);

        if (jobRequest.getId() != null && this.jobRequestRepo.exists(jobRequest.getId())) {
            throw new GenieConflictException("A job with id " + jobRequest.getId() + " already exists");
        }

        final JobRequestEntity jobRequestEntity = new JobRequestEntity();

        if (jobRequest.getId() != null) {
            jobRequestEntity.setId(jobRequest.getId());
        }
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

        jobRequestEntity.setDisableLogArchival(jobRequest.isDisableLogArchival());
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

        this.jobRequestRepo.save(jobRequestEntity);
        return jobRequestEntity.getDTO();
    }

    /**
     * Return the Job Entity for the job id provided.
     *
     * @param id The id of the job to return.
     * @return Job Execution details or null if not found
     * @throws GenieException if there is an error
     */
    @Override
    @Transactional(readOnly = true)
    public JobExecution getJobExecution(
        @NotBlank(message = "No id entered. Unable to get job request.")
        final String id
    ) throws GenieException {
        LOG.debug("Called");
        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepo.findOne(id);
        if (jobExecutionEntity != null) {
            return jobExecutionEntity.getDTO();
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobExecution(
        @NotNull(message = "Job Request is null so cannot be saved")
        final JobExecution jobExecution
    ) throws GenieException {
        LOG.debug("Called with jobExecution: {}", jobExecution);

        // Since a job request object should always exist before the job object is saved
        // the id should never be null
        if (StringUtils.isBlank(jobExecution.getId())) {
            throw new GenieConflictException("Cannot create a job execution entry with id blank or null");
        }

        if (this.jobExecutionRepo.exists(jobExecution.getId())) {
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
     * Method to set exit code for the job execution.
     *
     * @param id       the id of the job to update the exit code
     * @param exitCode The exit code of the process
     * @throws GenieException if there is an error
     */
    @Override
    public void setExitCode(
        @NotBlank(message = "No job id entered. Unable to update.")
        final String id,
        @NotBlank(message = "Exit code cannot be blank")
        final int exitCode
    ) throws GenieException {
        LOG.debug("Called");
        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepo.findOne(id);
        if (jobExecutionEntity != null) {
            jobExecutionEntity.setExitCode(exitCode);
            this.jobExecutionRepo.save(jobExecutionEntity);
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }
}
