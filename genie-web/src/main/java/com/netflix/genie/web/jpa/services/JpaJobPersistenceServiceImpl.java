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
package com.netflix.genie.web.jpa.services;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest;
import com.netflix.genie.common.internal.dto.v4.AgentEnvironmentRequest;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobRequestMetadata;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.CriterionEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.JobEntity;
import com.netflix.genie.web.jpa.entities.projections.IdProjection;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.TagPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
public class JpaJobPersistenceServiceImpl extends JpaBaseService implements JobPersistenceService {

    private final JpaJobRepository jobRepository;
    private final JpaApplicationRepository applicationRepository;
    private final JpaClusterRepository clusterRepository;
    private final JpaCommandRepository commandRepository;

    /**
     * Constructor.
     *
     * @param tagPersistenceService  The tag service to use
     * @param tagRepository          The tag repository to use
     * @param filePersistenceService The file service to use
     * @param fileRepository         The file repository to use
     * @param jobRepository          The job repository to use
     * @param applicationRepository  The application repository to use
     * @param clusterRepository      The cluster repository to use
     * @param commandRepository      The command repository to use
     */
    public JpaJobPersistenceServiceImpl(
        final TagPersistenceService tagPersistenceService,
        final JpaTagRepository tagRepository,
        final FilePersistenceService filePersistenceService,
        final JpaFileRepository fileRepository,
        final JpaJobRepository jobRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        super(tagPersistenceService, tagRepository, filePersistenceService, fileRepository);
        this.jobRepository = jobRepository;
        this.applicationRepository = applicationRepository;
        this.clusterRepository = clusterRepository;
        this.commandRepository = commandRepository;
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

        final ClusterEntity cluster = this.clusterRepository
            .findByUniqueId(clusterId)
            .orElseThrow(() -> new GenieNotFoundException("Cannot find cluster with ID " + clusterId));

        final CommandEntity command = this.commandRepository
            .findByUniqueId(commandId)
            .orElseThrow(() -> new GenieNotFoundException("Cannot find command with ID " + commandId));

        final List<ApplicationEntity> applications = Lists.newArrayList();
        for (final String applicationId : applicationIds) {
            final ApplicationEntity application = this.applicationRepository
                .findByUniqueId(applicationId)
                .orElseThrow(() -> new GenieNotFoundException("Cannot find application with ID + " + applicationId));
            applications.add(application);
        }

        job.setCluster(cluster);
        job.setCommand(command);
        job.setApplications(applications);

        // Save the amount of memory to allocate to the job
        job.setMemoryUsed(memory);
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
        jobEntity.setTimeout(timeout);
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
    public String saveJobRequest(
        @Valid final JobRequest jobRequest,
        @Valid final JobRequestMetadata jobRequestMetadata
    ) throws GenieException {
        log.debug("Attempting to save job request {} with request metadata {}", jobRequest, jobRequestMetadata);
        // TODO: Metrics
        final JobEntity jobEntity = new JobEntity();

        this.setUniqueId(jobEntity, jobRequest.getRequestedId().orElse(null));
        jobEntity.setCommandArgs(jobRequest.getCommandArgs());

        this.setJobMetadataFields(jobEntity, jobRequest.getMetadata());
        this.setExecutionEnvironmentFields(jobEntity, jobRequest.getResources());
        this.setExecutionResourceCriteriaFields(jobEntity, jobRequest.getCriteria());
        this.setRequestedAgentEnvironmentFields(jobEntity, jobRequest.getRequestedAgentEnvironment());
        this.setRequestedAgentConfigFields(jobEntity, jobRequest.getRequestedAgentConfig());
        this.setRequestMetadataFields(jobEntity, jobRequestMetadata);

        // Persist. Catch exception if the ID is reused
        try {
            return this.jobRepository.save(jobEntity).getUniqueId();
        } catch (final DataIntegrityViolationException e) {
            throw new GenieConflictException(
                "A job with id " + jobEntity.getUniqueId() + " already exists. Unable to reserve id.",
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobSpecification(
        @NotBlank(message = "Id is missing and is required") final String id,
        @Nullable final JobSpecification specification
    ) throws GenieException {

    }

    private void updateJobStatus(final JobEntity jobEntity, final JobStatus jobStatus, final String statusMsg) {
        final JobStatus status = jobEntity.getStatus();
        // Only change the status if the entity isn't already in a terminal state
        if (status.isActive()) {
            jobEntity.setStatus(jobStatus);
            jobEntity.setStatusMsg(statusMsg);

            if (jobStatus.equals(JobStatus.RUNNING)) {
                // Status being changed to running so set start date.
                jobEntity.setStarted(Instant.now());
            } else if (jobEntity.getStarted().isPresent() && jobStatus.isFinished()) {
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
    ) throws GenieException {
        final JobEntity jobEntity = new JobEntity();

        // Fields from the original Job Request

        jobEntity.setUniqueId(id);
        jobEntity.setName(jobRequest.getName());
        jobEntity.setUser(jobRequest.getUser());
        jobEntity.setVersion(jobRequest.getVersion());
        jobEntity.setStatus(JobStatus.INIT);
        jobRequest.getDescription().ifPresent(jobEntity::setDescription);
        JpaServiceUtils.setEntityMetadata(GenieObjectMapper.getMapper(), jobRequest, jobEntity);
        jobRequest.getCommandArgs().ifPresent(
            commandArgs ->
                jobEntity.setCommandArgs(
                    Lists.newArrayList(
                        StringUtils.splitByWholeSeparator(commandArgs, StringUtils.SPACE)
                    )
                )
        );
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
        jobExecution.getTimeout().ifPresent(jobEntity::setTimeout);
        jobExecution.getMemory().ifPresent(jobEntity::setMemoryUsed);

        return jobEntity;
    }

    private CriterionEntity toCriterionEntity(final Criterion criterion) throws GenieException {
        final CriterionEntity criterionEntity = new CriterionEntity();
        criterion.getId().ifPresent(criterionEntity::setUniqueId);
        criterion.getName().ifPresent(criterionEntity::setName);
        criterion.getVersion().ifPresent(criterionEntity::setVersion);
        criterion.getStatus().ifPresent(criterionEntity::setStatus);
        criterionEntity.setTags(this.createAndGetTagEntities(criterion.getTags()));
        return criterionEntity;
    }

    private void jsonToString(
        final JsonNode json,
        final Consumer<? super String> consumer
    ) {
        try {
            consumer.accept(GenieObjectMapper.getMapper().writeValueAsString(json));
        } catch (final JsonProcessingException jpe) {
            // TODO: Should never happen. Swallow for now till we decide what we want to do
            log.error("Invalid JSON, unable to convert {} to string", json, jpe);
            consumer.accept("{\"jsonProcessingException\": \"" + jpe.getMessage() + "\"}");
        }
    }

    private void setJobMetadataFields(
        final JobEntity jobEntity,
        final JobMetadata jobMetadata
    ) throws GenieException {
        // Required fields
        jobEntity.setName(jobMetadata.getName());
        jobEntity.setUser(jobMetadata.getUser());
        jobEntity.setVersion(jobMetadata.getVersion());

        // Optional fields
        jobEntity.setTags(this.createAndGetTagEntities(jobMetadata.getTags()));

        final Optional<JsonNode> jsonMetadata = jobMetadata.getMetadata();
        jsonMetadata.ifPresent(jsonNode -> this.jsonToString(jsonNode, jobEntity::setMetadata));
        jobMetadata.getDescription().ifPresent(jobEntity::setDescription);
        jobMetadata.getEmail().ifPresent(jobEntity::setEmail);
        jobMetadata.getGroup().ifPresent(jobEntity::setGenieUserGroup);
        jobMetadata.getGrouping().ifPresent(jobEntity::setGrouping);
        jobMetadata.getGroupingInstance().ifPresent(jobEntity::setGroupingInstance);
    }

    private void setExecutionEnvironmentFields(
        final JobEntity jobEntity,
        final ExecutionEnvironment executionEnvironment
    ) throws GenieException {
        final FileEntity setupFile = executionEnvironment.getSetupFile().isPresent()
            ? this.createAndGetFileEntity(executionEnvironment.getSetupFile().get())
            : null;
        if (setupFile != null) {
            jobEntity.setSetupFile(setupFile);
        }
        jobEntity.setConfigs(this.createAndGetFileEntities(executionEnvironment.getConfigs()));
        jobEntity.setDependencies(this.createAndGetFileEntities(executionEnvironment.getDependencies()));
    }

    private void setExecutionResourceCriteriaFields(
        final JobEntity jobEntity,
        final ExecutionResourceCriteria criteria
    ) throws GenieException {
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

    private void setRequestedAgentEnvironmentFields(
        final JobEntity jobEntity,
        final AgentEnvironmentRequest requestedAgentEnvironment
    ) throws GenieException {
        jobEntity.setRequestedEnvironmentVariables(requestedAgentEnvironment.getRequestedEnvironmentVariables());
        requestedAgentEnvironment.getRequestedJobMemory().ifPresent(jobEntity::setRequestedMemory);
        requestedAgentEnvironment.getRequestedJobCpu().ifPresent(jobEntity::setRequestedCpu);
        final Optional<JsonNode> agentEnvironmentExt = requestedAgentEnvironment.getExt();
        agentEnvironmentExt.ifPresent(
            jsonNode -> this.jsonToString(jsonNode, jobEntity::setRequestedAgentEnvironmentExt)
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
            jsonNode -> this.jsonToString(jsonNode, jobEntity::setRequestedAgentConfigExt)
        );
    }

    private void setRequestMetadataFields(
        final JobEntity jobEntity,
        final JobRequestMetadata jobRequestMetadata
    ) {
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
}
