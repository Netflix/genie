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

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobRequestMetadata;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.entities.JobRequestMetadataEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

/**
 * JPA implementation of the job persistence service.
 *
 * @author amsharma
 */
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
    private final JpaApplicationRepository applicationRepo;
    private final JpaClusterRepository clusterRepo;
    private final JpaCommandRepository commandRepo;

    /**
     * Constructor.
     *
     * @param jobRepo          The job repository to use
     * @param jobRequestRepo   The job request repository to use
     * @param jobExecutionRepo The jobExecution Repository to use
     * @param applicationRepo  The application repository to use
     * @param clusterRepo      The cluster repository to use
     * @param commandRepo      The command repository to use
     */
    public JpaJobPersistenceServiceImpl(
        final JpaJobRepository jobRepo,
        final JpaJobRequestRepository jobRequestRepo,
        final JpaJobExecutionRepository jobExecutionRepo,
        final JpaApplicationRepository applicationRepo,
        final JpaClusterRepository clusterRepo,
        final JpaCommandRepository commandRepo
    ) {
        this.jobRepo = jobRepo;
        this.jobRequestRepo = jobRequestRepo;
        this.jobExecutionRepo = jobExecutionRepo;
        this.applicationRepo = applicationRepo;
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
    public synchronized void updateJobStatus(
        @NotBlank(message = "No job id entered. Unable to update.")
        final String id,
        @NotNull(message = "Status cannot be null.")
        final JobStatus jobStatus,
        @NotBlank(message = "Status message cannot be empty.")
        final String statusMsg
    ) throws GenieException {
        log.debug("Called to update job with id {}, status {} and statusMsg \"{}\"", id, jobStatus, statusMsg);

        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity == null) {
            throw new GenieNotFoundException("No job exists for the id specified");
        }

        final JobStatus status = jobEntity.getStatus();
        // Only change the status if the entity isn't already in a terminal state
        if (status != JobStatus.FAILED && status != JobStatus.KILLED && status != JobStatus.SUCCEEDED) {
            jobEntity.setStatus(jobStatus);
            jobEntity.setStatusMsg(statusMsg);

            if (jobStatus.equals(JobStatus.RUNNING)) {
                // Status being changed to running so set start date.
                jobEntity.setStarted(new Date());
            } else if (jobEntity.getStarted() != null && (jobStatus.equals(JobStatus.KILLED)
                || jobStatus.equals(JobStatus.FAILED)
                || jobStatus.equals(JobStatus.SUCCEEDED))) {

                // Since start date is set the job was running previously and now has finished
                // with status killed, failed or succeeded. So we set the job finish time.
                jobEntity.setFinished(new Date());
            }
            this.jobRepo.save(jobEntity);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobWithRuntimeEnvironment(
        @NotBlank final String jobId,
        @NotBlank final String clusterId,
        @NotBlank final String commandId,
        @NotNull final List<String> applicationIds
    ) throws GenieException {
        log.debug(
            "Called to update job ({}) runtime with cluster {}, command {} and applications {}",
            jobId,
            clusterId,
            commandId,
            applicationIds
        );

        final JobEntity job = this.jobRepo.findOne(jobId);
        if (job == null) {
            throw new GenieNotFoundException("No job with id " + jobId + " exists.");
        }

        final ClusterEntity cluster = this.clusterRepo.findOne(clusterId);
        if (cluster == null) {
            throw new GenieNotFoundException("Cannot find cluster with ID " + clusterId);
        }

        final CommandEntity command = this.commandRepo.findOne(commandId);
        if (command == null) {
            throw new GenieNotFoundException("Cannot find command with ID " + commandId);
        }

        final List<ApplicationEntity> applications = Lists.newArrayList();
        for (final String applicationId : applicationIds) {
            final ApplicationEntity application = this.applicationRepo.findOne(applicationId);
            if (application == null) {
                throw new GenieNotFoundException("Cannot find application with ID + " + applicationId);
            }
            applications.add(application);
        }

        job.setCluster(cluster);
        job.setCommand(command);
        job.setApplications(applications);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequest createJobRequest(
        @NotNull final JobRequest jobRequest,
        @NotNull final JobRequestMetadata jobRequestMetadata
    ) throws GenieException {
        log.debug("Called with Job Request: {} and Job Request Metadata: {}", jobRequest, jobRequestMetadata);

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
        jobRequestEntity.setApplicationsFromList(jobRequest.getApplications());
        jobRequestEntity.setTimeout(jobRequest.getTimeout());

        final JobRequestMetadataEntity metadataEntity = new JobRequestMetadataEntity();
        metadataEntity.setClientHost(jobRequestMetadata.getClientHost());
        metadataEntity.setUserAgent(jobRequestMetadata.getUserAgent());
        metadataEntity.setNumAttachments(jobRequestMetadata.getNumAttachments());
        metadataEntity.setTotalSizeOfAttachments(jobRequestMetadata.getTotalSizeOfAttachments());

        jobRequestEntity.setJobRequestMetadata(metadataEntity);

        this.jobRequestRepo.save(jobRequestEntity);
        return jobRequestEntity.getDTO();
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

        this.updateJobStatus(jobExecution.getId(), JobStatus.RUNNING, "Job is Running.");
        final JobEntity jobEntity = this.jobRepo.findOne(jobExecution.getId());
        if (jobEntity == null) {
            throw new GenieNotFoundException("Cannot find the job for the id of the jobExecution specified.");
        }

        final JobExecutionEntity jobExecutionEntity = new JobExecutionEntity();

        jobExecutionEntity.setId(jobExecution.getId());
        jobExecutionEntity.setHostName(jobExecution.getHostName());
        jobExecutionEntity.setProcessId(jobExecution.getProcessId());
        jobExecutionEntity.setCheckDelay(jobExecution.getCheckDelay());
        jobExecutionEntity.setTimeout(jobExecution.getTimeout());

        jobEntity.setExecution(jobExecutionEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void setJobCompletionInformation(
        @NotBlank(message = "No job id entered. Unable to update.")
        final String id,
        final int exitCode,
        @NotNull(message = "No job status entered. Unable to update")
        final JobStatus status,
        @NotBlank(message = "Status message can't be blank. Unable to update")
        final String statusMessage
    ) throws GenieException {
        log.debug(
            "Called with id {}, exit code {}, status {} and status message {}", id, exitCode, status, statusMessage
        );
        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepo.findOne(id);
        if (jobExecutionEntity != null) {
            if (jobExecutionEntity.getExitCode() == JobExecution.DEFAULT_EXIT_CODE) {
                jobExecutionEntity.setExitCode(exitCode);
            }
            this.updateJobStatus(id, status, statusMessage);
        } else {
            throw new GenieNotFoundException("No job execution with ID " + id + " exists");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long deleteAllJobsCreatedBeforeDate(@NotNull final Date date) {
        return this.jobRequestRepo.deleteByCreatedBefore(date);
    }
}
