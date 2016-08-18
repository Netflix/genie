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
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
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
import com.netflix.genie.core.jpa.entities.JobMetadataEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobMetadataRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.constraints.Min;
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
    private final JpaJobMetadataRepository jobMetadataRepository;
    private final JpaApplicationRepository applicationRepo;
    private final JpaClusterRepository clusterRepo;
    private final JpaCommandRepository commandRepo;

    /**
     * Constructor.
     *
     * @param jobRepo               The job repository to use
     * @param jobRequestRepo        The job request repository to use
     * @param jobExecutionRepo      The jobExecution Repository to use
     * @param jobMetadataRepository The job metadata repository to use
     * @param applicationRepo       The application repository to use
     * @param clusterRepo           The cluster repository to use
     * @param commandRepo           The command repository to use
     */
    public JpaJobPersistenceServiceImpl(
        @NotNull final JpaJobRepository jobRepo,
        @NotNull final JpaJobRequestRepository jobRequestRepo,
        @NotNull final JpaJobExecutionRepository jobExecutionRepo,
        @NotNull final JpaJobMetadataRepository jobMetadataRepository,
        @NotNull final JpaApplicationRepository applicationRepo,
        @NotNull final JpaClusterRepository clusterRepo,
        @NotNull final JpaCommandRepository commandRepo
    ) {
        this.jobRepo = jobRepo;
        this.jobRequestRepo = jobRequestRepo;
        this.jobExecutionRepo = jobExecutionRepo;
        this.jobMetadataRepository = jobMetadataRepository;
        this.applicationRepo = applicationRepo;
        this.clusterRepo = clusterRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJobAndJobExecution(
        @NotNull(message = "No Job provided to create")
        final Job job,
        @NotNull(message = "No Job execution information provided. Unable to create.")
        final JobExecution jobExecution
    ) throws GenieException {
        log.debug("Called with job: {}", job);

        // Since a job request object should always exist before the job object is saved
        // the id should never be null
        final String jobId = job
            .getId()
            .orElseThrow(() -> new GeniePreconditionException("Cannot create a job without the id specified"));

        // check if job already exists in the database
        if (this.jobRepo.exists(jobId)) {
            throw new GenieConflictException("A job with id " + jobId + " already exists");
        }

        final JobRequestEntity jobRequestEntity = this.jobRequestRepo.findOne(jobId);
        if (jobRequestEntity == null) {
            throw new GeniePreconditionException("Cannot find the job request for the id of the job specified.");
        }

        final JobEntity jobEntity = new JobEntity();
        jobEntity.setId(jobId);
        jobEntity.setName(job.getName());
        jobEntity.setUser(job.getUser());
        jobEntity.setVersion(job.getVersion());
        job.getArchiveLocation().ifPresent(jobEntity::setArchiveLocation);
        job.getDescription().ifPresent(jobEntity::setDescription);
        job.getStarted().ifPresent(jobEntity::setStarted);
        jobEntity.setStatus(job.getStatus());
        job.getStatusMsg().ifPresent(jobEntity::setStatusMsg);
        jobEntity.setTags(job.getTags());
        jobEntity.setCommandArgs(job.getCommandArgs());

        final JobExecutionEntity jobExecutionEntity = new JobExecutionEntity();
        jobExecutionEntity.setHostName(jobExecution.getHostName());
        jobExecutionEntity.setId(jobId);
        jobExecution.getProcessId().ifPresent(jobExecutionEntity::setProcessId);
        jobExecution.getCheckDelay().ifPresent(jobExecutionEntity::setCheckDelay);
        jobExecution.getTimeout().ifPresent(jobExecutionEntity::setTimeout);

        // Persist the entities
        jobEntity.setExecution(jobExecutionEntity);
        jobRequestEntity.setJob(jobEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobStatus(
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
        if (status.isActive()) {
            jobEntity.setStatus(jobStatus);
            jobEntity.setStatusMsg(statusMsg);

            if (jobStatus.equals(JobStatus.RUNNING)) {
                // Status being changed to running so set start date.
                jobEntity.setStarted(new Date());
            } else if (jobEntity.getStarted().isPresent() && jobStatus.isFinished()) {
                // Since start date is set the job was running previously and now has finished
                // with status killed, failed or succeeded. So we set the job finish time.
                jobEntity.setFinished(new Date());
            }
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
        @NotNull final JobMetadata jobMetadata
    ) throws GenieException {
        log.debug("Called with Job Request: {} and Job Metadata: {}", jobRequest, jobMetadata);

        final String jobId = jobRequest.getId().orElseThrow(() -> new GeniePreconditionException("No job id entered"));

        if (this.jobRequestRepo.exists(jobId)) {
            throw new GenieConflictException("A job with id " + jobId + " already exists");
        }

        final JobRequestEntity jobRequestEntity = new JobRequestEntity();

        jobRequestEntity.setId(jobId);
        jobRequestEntity.setName(jobRequest.getName());
        jobRequestEntity.setUser(jobRequest.getUser());
        jobRequestEntity.setVersion(jobRequest.getVersion());
        jobRequest.getDescription().ifPresent(jobRequestEntity::setDescription);
        jobRequestEntity.setCommandArgs(jobRequest.getCommandArgs());
        jobRequest.getGroup().ifPresent(jobRequestEntity::setGroup);
        jobRequest.getSetupFile().ifPresent(jobRequestEntity::setSetupFile);
        jobRequestEntity.setClusterCriteriasFromList(jobRequest.getClusterCriterias());
        jobRequestEntity.setCommandCriteriaFromSet(jobRequest.getCommandCriteria());
        jobRequestEntity.setDependenciesFromSet(jobRequest.getDependencies());
        jobRequestEntity.setDisableLogArchival(jobRequest.isDisableLogArchival());
        jobRequest.getEmail().ifPresent(jobRequestEntity::setEmail);
        jobRequestEntity.setTags(jobRequest.getTags());
        jobRequest.getCpu().ifPresent(jobRequestEntity::setCpu);
        jobRequest.getMemory().ifPresent(jobRequestEntity::setMemory);
        jobRequestEntity.setApplicationsFromList(jobRequest.getApplications());
        jobRequest.getTimeout().ifPresent(jobRequestEntity::setTimeout);

        final JobMetadataEntity metadataEntity = new JobMetadataEntity();
        jobMetadata.getClientHost().ifPresent(metadataEntity::setClientHost);
        jobMetadata.getUserAgent().ifPresent(metadataEntity::setUserAgent);
        jobMetadata.getNumAttachments().ifPresent(metadataEntity::setNumAttachments);
        jobMetadata.getTotalSizeOfAttachments().ifPresent(metadataEntity::setTotalSizeOfAttachments);
        jobMetadata.getStdOutSize().ifPresent(metadataEntity::setStdOutSize);
        jobMetadata.getStdErrSize().ifPresent(metadataEntity::setStdErrSize);

        jobRequestEntity.setJobMetadata(metadataEntity);

        this.jobRequestRepo.save(jobRequestEntity);
        return jobRequestEntity.getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobRunningInformation(
        @NotBlank final String id,
        @Min(value = 0, message = "Must be no lower than zero") final int processId,
        @Min(value = 1, message = "Must be at least 1 millisecond, preferably much more") final long checkDelay,
        @NotNull final Date timeout
    ) throws GenieException {
        log.debug("Called with to update job {} with process id {}", id, processId);

        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepo.findOne(id);
        if (jobExecutionEntity == null) {
            throw new GenieNotFoundException("No job execution with id " + id + " exists. Unable to update.");
        }
        jobExecutionEntity.setProcessId(processId);
        jobExecutionEntity.setCheckDelay(checkDelay);
        jobExecutionEntity.setTimeout(timeout);
        this.updateJobStatus(id, JobStatus.RUNNING, "Job is Running.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobCompletionInformation(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        final int exitCode,
        @NotNull(message = "No job status entered. Unable to update") final JobStatus status,
        @NotBlank(message = "Status message can't be blank. Unable to update") final String statusMessage,
        @Nullable final Long stdOutSize,
        @Nullable final Long stdErrSize
    ) throws GenieException {
        log.debug(
            "Called with id {}, exit code {}, status {} and status message {}", id, exitCode, status, statusMessage
        );
        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepo.findOne(id);
        if (jobExecutionEntity != null) {
            if (!jobExecutionEntity.getExitCode().isPresent()) {
                jobExecutionEntity.setExitCode(exitCode);
            }
            this.updateJobStatus(id, status, statusMessage);
        } else {
            throw new GenieNotFoundException("No job execution with ID " + id + " exists");
        }

        // Save database query if we don't need it
        if (stdOutSize != null || stdErrSize != null) {
            final JobMetadataEntity jobMetadataEntity = this.jobMetadataRepository.findOne(id);
            if (jobMetadataEntity != null) {
                jobMetadataEntity.setStdOutSize(stdOutSize);
                jobMetadataEntity.setStdErrSize(stdErrSize);
            } else {
                throw new GenieNotFoundException("No job metadata for job with id " + id + " exists");
            }
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
