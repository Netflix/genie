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
package com.netflix.genie.web.data.services.jpa;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.FinishedJob;
import com.netflix.genie.common.internal.dto.v4.JobArchivalDataRequest;
import com.netflix.genie.common.internal.dto.v4.JobEnvironment;
import com.netflix.genie.common.internal.dto.v4.JobEnvironmentRequest;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobRequestMetadata;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.data.entities.ApplicationEntity;
import com.netflix.genie.web.data.entities.ClusterEntity;
import com.netflix.genie.web.data.entities.CommandEntity;
import com.netflix.genie.web.data.entities.CriterionEntity;
import com.netflix.genie.web.data.entities.FileEntity;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.projections.IdProjection;
import com.netflix.genie.web.data.entities.projections.JobApiProjection;
import com.netflix.genie.web.data.entities.projections.JobArchiveLocationProjection;
import com.netflix.genie.web.data.entities.projections.JobStatusProjection;
import com.netflix.genie.web.data.entities.projections.v4.FinishedJobProjection;
import com.netflix.genie.web.data.entities.projections.v4.IsV4JobProjection;
import com.netflix.genie.web.data.entities.projections.v4.JobSpecificationProjection;
import com.netflix.genie.web.data.entities.projections.v4.V4JobRequestProjection;
import com.netflix.genie.web.data.entities.v4.EntityDtoConverters;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaJobRepository;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.services.AttachmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JPA implementation of the job persistence service.
 *
 * @author amsharma
 * @author tgianos
 */
@Transactional(
    rollbackFor = {
        GenieException.class,
        GenieRuntimeException.class,
        ConstraintViolationException.class,
        IdAlreadyExistsException.class,
        SaveAttachmentException.class
    }
)
@Slf4j
public class JpaJobPersistenceServiceImpl extends JpaBaseService implements JobPersistenceService {

    private final JpaJobRepository jobRepository;
    private final AttachmentService attachmentService;

    /**
     * Constructor.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     * @param jobRepository          The {@link JpaJobRepository} to use
     * @param attachmentService      The {@link AttachmentService} implementation to use to persist attachments before
     *                               converting them to dependencies of the job
     */
    public JpaJobPersistenceServiceImpl(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository,
        final JpaJobRepository jobRepository,
        final AttachmentService attachmentService
    ) {
        super(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
        this.jobRepository = jobRepository;
        this.attachmentService = attachmentService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createJob(
        @NotNull final com.netflix.genie.common.dto.JobRequest jobRequest,
        @NotNull final com.netflix.genie.common.dto.JobMetadata jobMetadata,
        @NotNull final Job job,
        @NotNull final JobExecution jobExecution
    ) throws GenieException {
        log.debug(
            "Called with\nRequest:\n{}\nMetadata:\n{}\nJob:\n{}\nExecution:\n{}\n",
            jobRequest,
            jobMetadata,
            job,
            jobExecution
        );

        final String jobId = jobRequest.getId().orElseThrow(() -> new GeniePreconditionException("No job id entered"));
        final JobEntity jobEntity = this.toEntity(jobId, jobRequest, jobMetadata, job, jobExecution);
        try {
            this.jobRepository.save(jobEntity);
        } catch (final DataIntegrityViolationException e) {
            throw new GenieConflictException("A job with id " + jobId + " already exists", e);
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
        @NotNull final List<String> applicationIds,
        @Min(1) final int memory
    ) throws GenieException {
        log.debug(
            "Called to update job ({}) runtime with cluster {}, command {} and applications {}",
            jobId,
            clusterId,
            commandId,
            applicationIds
        );

        final JobEntity job = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + jobId + " exists."));
        try {
            this.setExecutionResources(job, clusterId, commandId, applicationIds);
        } catch (
            final GenieClusterNotFoundException
                | GenieCommandNotFoundException
                | GenieApplicationNotFoundException e
        ) {
            throw new GenieNotFoundException(e.getMessage(), e);
        }

        // Save the amount of memory to allocate to the job
        job.setMemoryUsed(memory);
        job.setResolved(true);
        // TODO: Should we set status to RESOLVED here? Not sure how that will work with V3 so leaving it INIT for now
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobStatus(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        @NotNull(message = "Status cannot be null.") final JobStatus jobStatus,
        @NotBlank(message = "Status message cannot be empty.") final String statusMsg
    ) throws GenieException {
        log.debug("Called to update job with id {}, status {} and statusMsg \"{}\"", id, jobStatus, statusMsg);

        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No job exists for the id specified"));

        this.updateJobStatus(jobEntity, jobStatus, statusMsg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobRunningInformation(
        @NotBlank final String id,
        @Min(value = 0, message = "Must be no lower than zero") final int processId,
        @Min(value = 1, message = "Must be at least 1 millisecond, preferably much more") final long checkDelay,
        @NotNull final Instant timeout
    ) throws GenieException {
        log.debug("Called with to update job {} with process id {}", id, processId);

        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exists. Unable to update"));

        this.updateJobStatus(jobEntity, JobStatus.RUNNING, "Job is Running.");
        jobEntity.setProcessId(processId);
        jobEntity.setCheckDelay(checkDelay);
        jobEntity
            .getStarted()
            .ifPresent(started -> jobEntity.setTimeoutUsed(this.toTimeoutUsed(started, timeout)));
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
            "Called with id: {}, exit code: {}, status: {}, status message: {}, std out size: {}, std err size {}",
            id,
            exitCode,
            status,
            statusMessage,
            stdOutSize,
            stdErrSize
        );
        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exists unable to update"));

        this.updateJobStatus(jobEntity, status, statusMessage);
        jobEntity.setExitCode(exitCode);
        jobEntity.setStdOutSize(stdOutSize);
        jobEntity.setStdErrSize(stdErrSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long deleteBatchOfJobsCreatedBeforeDate(
        @NotNull final Instant date,
        @Min(1) final int maxDeleted,
        @Min(1) final int pageSize
    ) {
        log.info(
            "Attempting to delete batch of jobs (at most {}) created before {} ms from epoch",
            maxDeleted,
            date.toEpochMilli()
        );
        long jobsDeleted = 0;
        long totalAttemptedDeletions = 0;
        final Pageable page = PageRequest.of(0, pageSize);
        Slice<IdProjection> idProjections;
        do {
            idProjections = this.jobRepository.findByCreatedBefore(date, page);
            if (idProjections.hasContent()) {
                final List<Long> ids = idProjections
                    .getContent()
                    .stream()
                    .map(IdProjection::getId)
                    .collect(Collectors.toList());

                final long toBeDeleted = ids.size();
                totalAttemptedDeletions += toBeDeleted;

                log.debug("Attempting to delete {} rows from jobs...", toBeDeleted);
                final long deletedJobs = this.jobRepository.deleteByIdIn(ids);
                log.debug("Successfully deleted {} rows from jobs...", deletedJobs);
                if (deletedJobs != toBeDeleted) {
                    log.error(
                        "Deleted {} job records but expected to delete {}",
                        deletedJobs,
                        toBeDeleted
                    );
                }
                jobsDeleted += deletedJobs;
            }
        } while (idProjections.hasNext() && totalAttemptedDeletions < maxDeleted);

        log.info(
            "Deleted a chunk of {} job records: {} job",
            totalAttemptedDeletions,
            jobsDeleted
        );
        return totalAttemptedDeletions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String saveJobSubmission(
        @Valid final JobSubmission jobSubmission
    ) throws IdAlreadyExistsException, SaveAttachmentException {
        log.debug("Attempting to save job submission {}", jobSubmission);
        // TODO: Metrics
        final JobEntity jobEntity = new JobEntity();

        final JobRequest jobRequest = jobSubmission.getJobRequest();
        final JobRequestMetadata jobRequestMetadata = jobSubmission.getJobRequestMetadata();

        // Create the unique id if one doesn't already exist
        this.setUniqueId(jobEntity, jobRequest.getRequestedId().orElse(null));

        // Do we have attachments? Save them so the agent can access them later.
        final Set<URI> attachmentURIs = this.attachmentService.saveAttachments(
            jobEntity.getUniqueId(),
            jobSubmission.getAttachments()
        );

        jobEntity.setCommandArgs(jobRequest.getCommandArgs());

        this.setJobMetadataFields(jobEntity, jobRequest.getMetadata());
        this.setExecutionEnvironmentFields(jobEntity, jobRequest.getResources(), attachmentURIs);
        this.setExecutionResourceCriteriaFields(jobEntity, jobRequest.getCriteria());
        this.setRequestedJobEnvironmentFields(jobEntity, jobRequest.getRequestedJobEnvironment());
        this.setRequestedAgentConfigFields(jobEntity, jobRequest.getRequestedAgentConfig());
        this.setRequestedJobArchivalData(jobEntity, jobRequest.getRequestedJobArchivalData());
        this.setRequestMetadataFields(jobEntity, jobRequestMetadata);

        // Flag to signal to rest of system that this job is V4. Temporary until everything moved to v4
        jobEntity.setV4(true);

        // Persist. Catch exception if the ID is reused
        try {
            final String id = this.jobRepository.save(jobEntity).getUniqueId();
            log.debug(
                "Saved job submission {} under job id {}",
                jobSubmission,
                id
            );
            return id;
        } catch (final DataIntegrityViolationException e) {
            throw new IdAlreadyExistsException(
                "A job with id " + jobEntity.getUniqueId() + " already exists. Unable to reserve id.",
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Optional<JobRequest> getJobRequest(@NotBlank(message = "Id is missing and is required") final String id) {
        log.debug("Requested to get Job Request for id {}", id);
        return this.jobRepository
            .findByUniqueId(id, V4JobRequestProjection.class)
            .map(EntityDtoConverters::toV4JobRequestDto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveResolvedJob(
        @NotBlank(message = "Id is missing and is required") final String id,
        @Valid final ResolvedJob resolvedJob
    ) {
        log.debug("Requested to save resolved information {} for job with id {}", resolvedJob, id);
        final JobEntity entity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(
                () -> {
                    final String error = "No job found with id " + id + ". Unable to save job specification";
                    log.error(error);
                    return new GenieJobNotFoundException(error);
                }
            );

        try {
            if (entity.isResolved()) {
                log.error(
                    "Attempted to save resolved job information {} for job {} that was already resolved",
                    resolvedJob,
                    id
                );
                // This job has already been resolved there's nothing further to save
                return;
            }
            // Make sure if the job is resolvable otherwise don't do anything
            if (!entity.getStatus().isResolvable()) {
                log.error(
                    "Job {} is already in a non-resolvable state {}. Needs to be one of {}. Won't save resolved info",
                    id,
                    entity.getStatus(),
                    JobStatus.getResolvableStatuses()
                );
                return;
            }
            final JobSpecification jobSpecification = resolvedJob.getJobSpecification();
            this.setExecutionResources(
                entity,
                jobSpecification.getCluster().getId(),
                jobSpecification.getCommand().getId(),
                jobSpecification
                    .getApplications()
                    .stream()
                    .map(JobSpecification.ExecutionResource::getId)
                    .collect(Collectors.toList())
            );

            entity.setEnvironmentVariables(jobSpecification.getEnvironmentVariables());
            entity.setJobDirectoryLocation(jobSpecification.getJobDirectoryLocation().getAbsolutePath());
            jobSpecification.getArchiveLocation().ifPresent(entity::setArchiveLocation);
            jobSpecification.getTimeout().ifPresent(entity::setTimeoutUsed);

            final JobEnvironment jobEnvironment = resolvedJob.getJobEnvironment();
            entity.setMemoryUsed(jobEnvironment.getMemory());

            // TODO: There's probably some other fields we want to use from jobEnvironment

            entity.setResolved(true);
            entity.setStatus(JobStatus.RESOLVED);
            log.debug("Saved resolved information {} for job with id {}", resolvedJob, id);
        } catch (
            final GenieApplicationNotFoundException
                | GenieCommandNotFoundException
                | GenieClusterNotFoundException e
        ) {
            log.error(
                "Unable to save resolved job information {} for job {} due to {}",
                resolvedJob,
                id,
                e.getMessage(),
                e
            );
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Optional<JobSpecification> getJobSpecification(
        @NotBlank(message = "Id is missing and is required") final String id
    ) {
        log.debug("Requested to get job specification for job {}", id);
        final JobSpecificationProjection projection = this.jobRepository
            .findByUniqueId(id, JobSpecificationProjection.class)
            .orElseThrow(
                () -> {
                    final String errorMessage = "No job ith id " + id + "exists. Unable to get job specification.";
                    log.error(errorMessage);
                    return new GenieJobNotFoundException(errorMessage);
                }
            );

        return projection.isResolved()
            ? Optional.of(EntityDtoConverters.toJobSpecificationDto(projection))
            : Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    // TODO: The AOP aspects are firing on a lot of these APIs for retries and we may not want them to given a lot of
    //       these are un-recoverable. May want to revisit what is in the aspect.
    @Override
    public void claimJob(
        @NotBlank(message = "Job id is missing and is required") final String id,
        final @Valid AgentClientMetadata agentClientMetadata
    ) {
        log.debug("Agent with metadata {} requesting to claim job with id {}", agentClientMetadata, id);
        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieJobNotFoundException("No job with id " + id + " exists. Unable to claim."));

        if (jobEntity.isClaimed()) {
            throw new GenieJobAlreadyClaimedException("Job with id " + id + " is already claimed. Unable to claim.");
        }

        final JobStatus currentStatus = jobEntity.getStatus();
        // The job must be in one of the claimable states in order to be claimed
        // TODO: Perhaps could use jobEntity.isResolved here also but wouldn't check the case that the job was in a
        //       terminal state like killed or invalid in which case we shouldn't claim it anyway as the agent would
        //       continue running
        if (!currentStatus.isClaimable()) {
            throw new GenieInvalidStatusException(
                "Job "
                    + id
                    + " is in status "
                    + currentStatus
                    + " and can't be claimed. Needs to be one of "
                    + JobStatus.getClaimableStatuses()
            );
        }

        // Good to claim
        jobEntity.setClaimed(true);
        jobEntity.setStatus(JobStatus.CLAIMED);
        // TODO: It might be nice to set the status message as well to something like "Job claimed by XYZ..."
        //       we could do this in other places too like after reservation, resolving, etc

        // TODO: Should these be required? We're reusing the DTO here but perhaps the expectation at this point
        //       is that the agent will always send back certain metadata
        agentClientMetadata.getHostname().ifPresent(jobEntity::setAgentHostname);
        agentClientMetadata.getVersion().ifPresent(jobEntity::setAgentVersion);
        agentClientMetadata.getPid().ifPresent(jobEntity::setAgentPid);
        log.debug("Claimed job {} for agent with metadata {}", id, agentClientMetadata);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobStatus(
        @NotBlank(message = "Id is missing and is required") final String id,
        @NotNull final JobStatus currentStatus,
        @NotNull final JobStatus newStatus,
        @Nullable final String newStatusMessage
    ) {
        log.debug(
            "Requested to change the status of job {} from {} to {} with message {}",
            id,
            currentStatus,
            newStatus,
            newStatusMessage
        );
        if (currentStatus == newStatus) {
            throw new GenieInvalidStatusException(
                "Can't update the status of job " + id + " because both current and new status are " + currentStatus
            );
        }

        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(
                () -> new GenieJobNotFoundException("No job with id " + id + " exists. Unable to update status.")
            );

        final JobStatus actualCurrentStatus = jobEntity.getStatus();
        if (actualCurrentStatus != currentStatus) {
            throw new GenieInvalidStatusException(
                "Job "
                    + id
                    + " current status is "
                    + actualCurrentStatus
                    + " but API caller expected it to be "
                    + currentStatus
                    + ". Unable to update status due to inconsistent state."
            );
        }

        // TODO: Should we throw an exception if the job is already in a terminal state and someone is trying to
        //       further update it? In the private method below used in Genie 3 it's just swallowed and is a no-op

        // TODO: Should we prevent updating status for statuses already covered by "reserveJobId" and
        //      "saveResolvedJob"?

        this.updateJobStatus(jobEntity, newStatus, newStatusMessage);

        log.debug(
            "Changed the status of job {} from {} to {} with message {}",
            id,
            currentStatus,
            newStatus,
            newStatusMessage
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public boolean isV4(
        @NotBlank(message = "Id is missing and is required") final String id
    ) {
        log.debug("Read v4 flag from db for job {} ", id);
        return this.jobRepository
            .findByUniqueId(id, IsV4JobProjection.class)
            .orElseThrow(
                () -> {
                    final String errorMessage = "No job with id " + id + "exists. Unable to get v4 flag.";
                    log.error(errorMessage);
                    return new GenieJobNotFoundException(errorMessage);
                }
            ).isV4();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public JobStatus getJobStatus(
        @NotBlank(message = "Job id is missing and is required") final String id
    ) throws GenieNotFoundException {
        return this.jobRepository
            .findByUniqueId(id, JobStatusProjection.class)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exists. Unable to get status."))
            .getStatus();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Optional<String> getJobArchiveLocation(
        @NotBlank(message = "Job id is missing and is required") final String id
    ) throws GenieNotFoundException {
        return this.jobRepository
            .findByUniqueId(id, JobArchiveLocationProjection.class)
            .map(JobArchiveLocationProjection::getArchiveLocation)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exits."));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public FinishedJob getFinishedJob(
        @NotBlank(message = "Job id is missing and is required") final String id
    ) throws GenieNotFoundException, GenieInvalidStatusException {
        return this.jobRepository.findByUniqueId(id, FinishedJobProjection.class)
            .map(EntityDtoConverters::toFinishedJobDto)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exists."));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public boolean isApiJob(
        @NotBlank(message = "Job id is missing and is required") final String id
    ) throws GenieNotFoundException {
        return this.jobRepository
            .findByUniqueId(id, JobApiProjection.class)
            .map(JobApiProjection::isApi)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exists"));
    }

    private void updateJobStatus(
        final JobEntity jobEntity,
        final JobStatus newStatus,
        @Nullable final String statusMsg
    ) {
        final JobStatus currentStatus = jobEntity.getStatus();
        // Only change the status if the entity isn't already in a terminal state
        if (currentStatus.isActive()) {
            jobEntity.setStatus(newStatus);
            jobEntity.setStatusMsg(statusMsg);

            if (newStatus.equals(JobStatus.RUNNING)) {
                // Status being changed to running so set start date.
                jobEntity.setStarted(Instant.now());
            } else if (jobEntity.getStarted().isPresent() && newStatus.isFinished()) {
                // Since start date is set the job was running previously and now has finished
                // with status killed, failed or succeeded. So we set the job finish time.
                jobEntity.setFinished(Instant.now());
            }
        }
    }

    private JobEntity toEntity(
        final String id,
        final com.netflix.genie.common.dto.JobRequest jobRequest,
        final com.netflix.genie.common.dto.JobMetadata jobMetadata,
        final Job job,
        final JobExecution jobExecution
    ) {
        final JobEntity jobEntity = new JobEntity();

        // Fields from the original Job Request

        jobEntity.setUniqueId(id);
        jobEntity.setName(jobRequest.getName());
        jobEntity.setUser(jobRequest.getUser());
        jobEntity.setVersion(jobRequest.getVersion());
        jobEntity.setStatus(JobStatus.INIT);
        jobRequest.getDescription().ifPresent(jobEntity::setDescription);
        jobRequest
            .getMetadata()
            .ifPresent(metadata -> EntityDtoConverters.setJsonField(metadata, jobEntity::setMetadata));
        JpaServiceUtils.setEntityMetadata(GenieObjectMapper.getMapper(), jobRequest, jobEntity);
        jobRequest.getCommandArgs().ifPresent(commandArgs -> jobEntity.setCommandArgs(Lists.newArrayList(commandArgs)));
        jobRequest.getGroup().ifPresent(jobEntity::setGenieUserGroup);
        final FileEntity setupFile = jobRequest.getSetupFile().isPresent()
            ? this.createAndGetFileEntity(jobRequest.getSetupFile().get())
            : null;
        if (setupFile != null) {
            jobEntity.setSetupFile(setupFile);
        }
        final List<CriterionEntity> clusterCriteria
            = Lists.newArrayListWithExpectedSize(jobRequest.getClusterCriterias().size());

        for (final ClusterCriteria clusterCriterion : jobRequest.getClusterCriterias()) {
            clusterCriteria.add(
                new CriterionEntity(
                    null,
                    null,
                    null,
                    null,
                    this.createAndGetTagEntities(clusterCriterion.getTags())
                )
            );
        }
        jobEntity.setClusterCriteria(clusterCriteria);

        jobEntity.setCommandCriterion(
            new CriterionEntity(
                null,
                null,
                null,
                null,
                this.createAndGetTagEntities(jobRequest.getCommandCriteria())
            )
        );
        jobEntity.setConfigs(this.createAndGetFileEntities(jobRequest.getConfigs()));
        jobEntity.setDependencies(this.createAndGetFileEntities(jobRequest.getDependencies()));
        jobEntity.setArchivingDisabled(jobRequest.isDisableLogArchival());
        jobRequest.getEmail().ifPresent(jobEntity::setEmail);
        if (!jobRequest.getTags().isEmpty()) {
            jobEntity.setTags(this.createAndGetTagEntities(jobRequest.getTags()));
        }
        jobRequest.getCpu().ifPresent(jobEntity::setRequestedCpu);
        jobRequest.getMemory().ifPresent(jobEntity::setRequestedMemory);
        if (!jobRequest.getApplications().isEmpty()) {
            jobEntity.setRequestedApplications(jobRequest.getApplications());
        }
        jobRequest.getTimeout().ifPresent(jobEntity::setRequestedTimeout);

        jobRequest.getGrouping().ifPresent(jobEntity::setGrouping);
        jobRequest.getGroupingInstance().ifPresent(jobEntity::setGroupingInstance);

        // Fields collected as metadata

        jobMetadata.getClientHost().ifPresent(jobEntity::setRequestApiClientHostname);
        jobMetadata.getUserAgent().ifPresent(jobEntity::setRequestApiClientUserAgent);
        jobMetadata.getNumAttachments().ifPresent(jobEntity::setNumAttachments);
        jobMetadata.getTotalSizeOfAttachments().ifPresent(jobEntity::setTotalSizeOfAttachments);
        jobMetadata.getStdErrSize().ifPresent(jobEntity::setStdErrSize);
        jobMetadata.getStdOutSize().ifPresent(jobEntity::setStdOutSize);
        // For V3 (which this method supports) it's always API
        jobEntity.setApi(true);

        // Fields a user cares about (job dto)

        job.getArchiveLocation().ifPresent(jobEntity::setArchiveLocation);
        job.getStarted().ifPresent(jobEntity::setStarted);
        job.getFinished().ifPresent(jobEntity::setFinished);
        jobEntity.setStatus(job.getStatus());
        job.getStatusMsg().ifPresent(jobEntity::setStatusMsg);

        // Fields set by system as part of job execution
        jobEntity.setAgentHostname(jobExecution.getHostName());
        jobExecution.getProcessId().ifPresent(jobEntity::setProcessId);
        jobExecution.getCheckDelay().ifPresent(jobEntity::setCheckDelay);
        if (job.getStarted().isPresent() && jobExecution.getTimeout().isPresent()) {
            jobEntity.setTimeoutUsed(this.toTimeoutUsed(job.getStarted().get(), jobExecution.getTimeout().get()));
        }
        jobExecution.getMemory().ifPresent(jobEntity::setMemoryUsed);

        // Flag to signal to rest of system that this job is V3. Temporary until everything moved to v4
        jobEntity.setV4(false);

        return jobEntity;
    }

    private CriterionEntity toCriterionEntity(final Criterion criterion) {
        final CriterionEntity criterionEntity = new CriterionEntity();
        criterion.getId().ifPresent(criterionEntity::setUniqueId);
        criterion.getName().ifPresent(criterionEntity::setName);
        criterion.getVersion().ifPresent(criterionEntity::setVersion);
        criterion.getStatus().ifPresent(criterionEntity::setStatus);
        criterionEntity.setTags(this.createAndGetTagEntities(criterion.getTags()));
        return criterionEntity;
    }

    private void setJobMetadataFields(final JobEntity jobEntity, final JobMetadata jobMetadata) {
        // Required fields
        jobEntity.setName(jobMetadata.getName());
        jobEntity.setUser(jobMetadata.getUser());
        jobEntity.setVersion(jobMetadata.getVersion());

        // Optional fields
        jobEntity.setTags(this.createAndGetTagEntities(jobMetadata.getTags()));

        final Optional<JsonNode> jsonMetadata = jobMetadata.getMetadata();
        jsonMetadata.ifPresent(jsonNode -> EntityDtoConverters.setJsonField(jsonNode, jobEntity::setMetadata));
        jobMetadata.getDescription().ifPresent(jobEntity::setDescription);
        jobMetadata.getEmail().ifPresent(jobEntity::setEmail);
        jobMetadata.getGroup().ifPresent(jobEntity::setGenieUserGroup);
        jobMetadata.getGrouping().ifPresent(jobEntity::setGrouping);
        jobMetadata.getGroupingInstance().ifPresent(jobEntity::setGroupingInstance);
    }

    private void setExecutionEnvironmentFields(
        final JobEntity jobEntity,
        final ExecutionEnvironment executionEnvironment,
        @Nullable final Set<URI> savedAttachments
    ) {
        final FileEntity setupFile = executionEnvironment.getSetupFile().isPresent()
            ? this.createAndGetFileEntity(executionEnvironment.getSetupFile().get())
            : null;
        if (setupFile != null) {
            jobEntity.setSetupFile(setupFile);
        }
        jobEntity.setConfigs(this.createAndGetFileEntities(executionEnvironment.getConfigs()));
        final Set<FileEntity> dependencies = this.createAndGetFileEntities(executionEnvironment.getDependencies());
        if (savedAttachments != null) {
            dependencies.addAll(
                this.createAndGetFileEntities(
                    savedAttachments.stream().map(URI::toString).collect(Collectors.toSet())
                )
            );
        }
        jobEntity.setDependencies(dependencies);
    }

    private void setExecutionResourceCriteriaFields(
        final JobEntity jobEntity,
        final ExecutionResourceCriteria criteria
    ) {
        final List<Criterion> clusterCriteria = criteria.getClusterCriteria();
        final List<CriterionEntity> clusterCriteriaEntities
            = Lists.newArrayListWithExpectedSize(clusterCriteria.size());

        for (final Criterion clusterCriterion : clusterCriteria) {
            clusterCriteriaEntities.add(this.toCriterionEntity(clusterCriterion));
        }
        jobEntity.setClusterCriteria(clusterCriteriaEntities);
        jobEntity.setCommandCriterion(this.toCriterionEntity(criteria.getCommandCriterion()));
        jobEntity.setRequestedApplications(criteria.getApplicationIds());
    }

    private void setRequestedJobEnvironmentFields(
        final JobEntity jobEntity,
        final JobEnvironmentRequest requestedJobEnvironment
    ) {
        jobEntity.setRequestedEnvironmentVariables(requestedJobEnvironment.getRequestedEnvironmentVariables());
        requestedJobEnvironment.getRequestedJobMemory().ifPresent(jobEntity::setRequestedMemory);
        requestedJobEnvironment.getRequestedJobCpu().ifPresent(jobEntity::setRequestedCpu);
        final Optional<JsonNode> agentEnvironmentExt = requestedJobEnvironment.getExt();
        agentEnvironmentExt.ifPresent(
            jsonNode -> EntityDtoConverters.setJsonField(jsonNode, jobEntity::setRequestedAgentEnvironmentExt)
        );
    }

    private void setRequestedAgentConfigFields(
        final JobEntity jobEntity,
        final AgentConfigRequest requestedAgentConfig
    ) {
        jobEntity.setInteractive(requestedAgentConfig.isInteractive());
        jobEntity.setArchivingDisabled(requestedAgentConfig.isArchivingDisabled());
        requestedAgentConfig
            .getRequestedJobDirectoryLocation()
            .ifPresent(location -> jobEntity.setRequestedJobDirectoryLocation(location.getAbsolutePath()));
        requestedAgentConfig.getTimeoutRequested().ifPresent(jobEntity::setRequestedTimeout);
        requestedAgentConfig.getExt().ifPresent(
            jsonNode -> EntityDtoConverters.setJsonField(jsonNode, jobEntity::setRequestedAgentConfigExt)
        );
    }

    private void setRequestedJobArchivalData(
        final JobEntity jobEntity,
        final JobArchivalDataRequest requestedjobArchivalData
    ) {
        requestedjobArchivalData
            .getRequestedArchiveLocationPrefix()
            .ifPresent(jobEntity::setRequestedArchiveLocationPrefix);
    }

    private void setRequestMetadataFields(
        final JobEntity jobEntity,
        final JobRequestMetadata jobRequestMetadata
    ) {
        jobEntity.setApi(jobRequestMetadata.isApi());
        jobEntity.setNumAttachments(jobRequestMetadata.getNumAttachments());
        jobEntity.setTotalSizeOfAttachments(jobRequestMetadata.getTotalSizeOfAttachments());
        jobRequestMetadata.getApiClientMetadata().ifPresent(
            apiClientMetadata -> {
                apiClientMetadata.getHostname().ifPresent(jobEntity::setRequestApiClientHostname);
                apiClientMetadata.getUserAgent().ifPresent(jobEntity::setRequestApiClientUserAgent);
            }
        );
        jobRequestMetadata.getAgentClientMetadata().ifPresent(
            agentClientMetadata -> {
                agentClientMetadata.getHostname().ifPresent(jobEntity::setRequestAgentClientHostname);
                agentClientMetadata.getVersion().ifPresent(jobEntity::setRequestAgentClientVersion);
                agentClientMetadata.getPid().ifPresent(jobEntity::setRequestAgentClientPid);
            }
        );
    }

    private void setExecutionResources(
        final JobEntity job,
        final String clusterId,
        final String commandId,
        final List<String> applicationIds
    ) {
        final ClusterEntity cluster = this.getClusterEntity(clusterId).orElseThrow(
            () -> new GenieClusterNotFoundException("Cannot find cluster with ID " + clusterId)
        );

        final CommandEntity command = this.getCommandEntity(commandId).orElseThrow(
            () -> new GenieCommandNotFoundException("Cannot find command with ID " + commandId)
        );

        final List<ApplicationEntity> applications = Lists.newArrayList();
        for (final String applicationId : applicationIds) {
            final ApplicationEntity application = this.getApplicationEntity(applicationId).orElseThrow(
                () -> new GenieApplicationNotFoundException("Cannot find application with ID + " + applicationId)
            );
            applications.add(application);
        }

        job.setCluster(cluster);
        job.setCommand(command);
        job.setApplications(applications);
    }

    private int toTimeoutUsed(final Instant started, final Instant timeout) {
        return (int) started.until(timeout, ChronoUnit.SECONDS);
    }
}
