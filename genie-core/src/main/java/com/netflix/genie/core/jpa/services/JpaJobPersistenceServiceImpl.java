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
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.constraints.NotNull;
import java.util.Date;

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
@Slf4j
public class JpaJobPersistenceServiceImpl implements JobPersistenceService {

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
    public void createJob(
        @NotNull(message = "No Job provided to create")
        final Job job
    ) throws GenieException {
        log.debug("Called with job: {}", job);

        // Since a job request object should always exist before the job object is saved
        // the id should never be null
        if (StringUtils.isBlank(job.getId())) {
            throw new GeniePreconditionException("Cannot create a job without the id specified");
        }

        // check if job already exists in the database
        if (this.jobRepo.exists(job.getId())) {
            throw new GenieConflictException("A job with id " + job.getId() + " already exists");
        }

        final JobRequestEntity jobRequestEntity = this.jobRequestRepo.findOne(job.getId());

        if (jobRequestEntity == null) {
            throw new GeniePreconditionException("Cannot find the job request for the id of the job specified.");
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
        jobEntity.setTags(job.getTags());
        jobEntity.setCommandArgs(job.getCommandArgs());

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

        log.debug("Called to update job with id {}, status {} and statusMsg \"{}\"", id, jobStatus, statusMsg);

        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity == null) {
            throw new GenieNotFoundException("No job exists for the id specified");
        }

        jobEntity.setStatus(jobStatus);
        jobEntity.setStatusMsg(statusMsg);

        // If the status is either failed, killed or succeeded then set the finish time of the job as well
        if (jobStatus.equals(JobStatus.KILLED)
            || jobStatus.equals(JobStatus.FAILED)
            || jobStatus.equals(JobStatus.SUCCEEDED)) {

            jobEntity.setFinished(new Date());
        }
        this.jobRepo.save(jobEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateClusterForJob(
        @NotBlank(message = "Job id cannot be null while updating cluster information")
        final String jobId,
        @NotBlank(message = "Cluster id cannot be null while updating cluster information")
        final String clusterId
    ) throws GenieException {
        log.debug("Called with jobId {} and clusterID {}", jobId, clusterId);

        final JobEntity jobEntity = this.jobRepo.findOne(jobId);
        if (jobEntity == null) {
            throw new GenieNotFoundException("Cannot find job with ID " + jobId);
        }

        final ClusterEntity clusterEntity = this.clusterRepo.findOne(clusterId);
        if (clusterEntity == null) {
            throw new GenieNotFoundException("Cannot find cluster with ID " + clusterId);
        }

        jobEntity.setCluster(clusterEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCommandForJob(
        @NotBlank(message = "Job id cannot be null while updating command information")
        final String jobId,
        @NotBlank(message = "Command id cannot be null while updating command information")
        final String commandId
    ) throws GenieException {
        log.debug("Called with jobId {} and commandId {}", jobId, commandId);

        final JobEntity jobEntity = this.jobRepo.findOne(jobId);
        if (jobEntity == null) {
            throw new GenieNotFoundException("Cannot find job with ID " + jobId);
        }

        final CommandEntity commandEntity = this.commandRepo.findOne(commandId);
        if (commandEntity == null) {
            throw new GenieNotFoundException("Cannot find command with ID " + commandId);
        }

        jobEntity.setCommand(commandEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequest createJobRequest(
        @NotNull(message = "Job Request is null so cannot be saved")
        final JobRequest jobRequest
    ) throws GenieException {
        log.debug("Called with jobRequest: {}", jobRequest);

        if (jobRequest.getId() != null && this.jobRequestRepo.exists(jobRequest.getId())) {
            throw new GenieConflictException("A job with id " + jobRequest.getId() + " already exists");
        }

        final JobRequestEntity jobRequestEntity = new JobRequestEntity();

        jobRequestEntity.setId(jobRequest.getId());
        jobRequestEntity.setName(jobRequest.getName());
        jobRequestEntity.setUser(jobRequest.getUser());
        jobRequestEntity.setVersion(jobRequest.getVersion());
        jobRequestEntity.setDescription(jobRequest.getDescription());
        jobRequestEntity.setCommandArgs(jobRequest.getCommandArgs());
        jobRequestEntity.setGroup(jobRequest.getGroup());
        jobRequestEntity.setSetupFile(jobRequest.getSetupFile());
        jobRequestEntity.setClusterCriteriasFromList(jobRequest.getClusterCriterias());
        jobRequestEntity.setCommandCriteriaFromSet(jobRequest.getCommandCriteria());
        jobRequestEntity.setDependenciesFromSet(jobRequest.getDependencies());
        jobRequestEntity.setDisableLogArchival(jobRequest.isDisableLogArchival());
        jobRequestEntity.setEmail(jobRequest.getEmail());
        jobRequestEntity.setTags(jobRequest.getTags());
        jobRequestEntity.setCpu(jobRequest.getCpu());
        jobRequestEntity.setMemory(jobRequest.getMemory());

        this.jobRequestRepo.save(jobRequestEntity);
        return jobRequestEntity.getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addClientHostToJobRequest(
        @NotNull(message = "job request id not provided.")
        final String id,
        @NotBlank(message = "client host cannot be null")
        final String clientHost)
        throws GenieException {

        log.debug("Called with id: {} and client host: {}", id, clientHost);

        final JobRequestEntity jobRequestEntity = this.jobRequestRepo.findOne(id);
        if (jobRequestEntity == null) {
            throw new GenieNotFoundException("Cannot find the job request for id: " + id);
        }

        jobRequestEntity.setClientHost(clientHost);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobExecution(
        @NotNull(message = "Job Request is null so cannot be saved")
        final JobExecution jobExecution
    ) throws GenieException {
        log.debug("Called with jobExecution: {}", jobExecution);

        // Since a job request object should always exist before the job object is saved
        // the id should never be null
        if (StringUtils.isBlank(jobExecution.getId())) {
            throw new GeniePreconditionException("Cannot create a job execution entry with id blank or null");
        }

        final JobEntity jobEntity = jobRepo.findOne(jobExecution.getId());
        if (jobEntity == null) {
            throw new GenieNotFoundException("Cannot find the job for the id of the jobExecution specified.");
        }

        final JobExecutionEntity jobExecutionEntity = new JobExecutionEntity();

        jobExecutionEntity.setId(jobExecution.getId());
        jobExecutionEntity.setHostname(jobExecution.getHostname());
        jobExecutionEntity.setProcessId(jobExecution.getProcessId());
        jobExecutionEntity.setCheckDelay(jobExecution.getCheckDelay());

        jobEntity.setExecution(jobExecutionEntity);
        jobEntity.setStatus(JobStatus.RUNNING);
        jobEntity.setStatusMsg("Job is Running");
        jobEntity.setStarted(new Date());
    }

    /**
     * Method to set exit code for the job execution.
     *
     * @param id       the id of the job to update the exit code
     * @param exitCode The exit code of the process
     * @throws GenieException if there is an error
     */
    @Override
    public synchronized void setExitCode(
        @NotBlank(message = "No job id entered. Unable to update.")
        final String id,
        @NotBlank(message = "Exit code cannot be blank")
        final int exitCode
    ) throws GenieException {
        log.debug("Called with id {} and exit code {}", id, exitCode);

        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepo.findOne(id);
        if (jobExecutionEntity != null) {

            // Make sure current exit code is the default one before updating
            if (jobExecutionEntity.getExitCode() == JobExecution.DEFAULT_EXIT_CODE) {
                switch (exitCode) {
                    // No update to status in case of default exit code
                    case JobExecution.DEFAULT_EXIT_CODE:
                        break;
                    case JobExecution.KILLED_EXIT_CODE:
                        this.updateJobStatus(id, JobStatus.KILLED, "Job killed.");
                        break;
                    case JobExecution.ZOMBIE_EXIT_CODE:
                        this.updateJobStatus(id, JobStatus.FAILED, "Job marked as zombie by genie.");
                        break;
                    case JobExecution.SUCCESS_EXIT_CODE:
                        this.updateJobStatus(id, JobStatus.SUCCEEDED, "Job finished successfully.");
                        break;
                    // catch all for non-zero and non zombie, killed and failed exit codes
                    default:
                        this.updateJobStatus(id, JobStatus.FAILED, "Job failed.");
                }
                jobExecutionEntity.setExitCode(exitCode);
            } else {
                throw new GeniePreconditionException("Exit code already changed from default. Cannot update.");
            }

        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }
}
