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
import com.netflix.genie.common.dto.ClusterCriteria;
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
import com.netflix.genie.core.jpa.entities.CriterionEntity;
import com.netflix.genie.core.jpa.entities.FileEntity;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.projections.IdProjection;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaTagRepository;
import com.netflix.genie.core.services.FileService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.TagService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
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
     * @param tagService            The tag service to use
     * @param tagRepository         The tag repository to use
     * @param fileService           The file service to use
     * @param fileRepository        The file repository to use
     * @param jobRepository         The job repository to use
     * @param applicationRepository The application repository to use
     * @param clusterRepository     The cluster repository to use
     * @param commandRepository     The command repository to use
     */
    public JpaJobPersistenceServiceImpl(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaJobRepository jobRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        super(tagService, tagRepository, fileService, fileRepository);
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
        @NotNull final JobRequest jobRequest,
        @NotNull final JobMetadata jobMetadata,
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
        if (this.jobRepository.existsByUniqueId(jobId)) {
            throw new GenieConflictException("A job with id " + jobId + " already exists");
        }

        final JobEntity jobEntity = this.toEntity(jobId, jobRequest, jobMetadata, job, jobExecution);
        this.jobRepository.save(jobEntity);
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
        @NotNull final Date timeout
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
        @NotNull final Date date,
        @Min(1) final int maxDeleted,
        @Min(1) final int pageSize
    ) {
        log.info(
            "Attempting to delete batch of jobs (at most {}) created before {} ms from epoch",
            maxDeleted,
            date.getTime()
        );
        long jobsDeleted = 0;
        long totalAttemptedDeletions = 0;
        final Pageable page = new PageRequest(0, pageSize);
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

    private void updateJobStatus(final JobEntity jobEntity, final JobStatus jobStatus, final String statusMsg) {
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

    private JobEntity toEntity(
        final String id,
        final JobRequest jobRequest,
        final JobMetadata jobMetadata,
        final Job job,
        final JobExecution jobExecution
    ) throws GenieException {
        final JobEntity jobEntity = new JobEntity();

        // Fields from the original Job Request

        jobEntity.setUniqueId(id);
        jobEntity.setName(jobRequest.getName());
        jobEntity.setUser(jobRequest.getUser());
        jobEntity.setVersion(jobRequest.getVersion());
        jobRequest.getDescription().ifPresent(jobEntity::setDescription);
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
            clusterCriteria.add(new CriterionEntity(this.createAndGetTagEntities(clusterCriterion.getTags())));
        }
        jobEntity.setClusterCriteria(clusterCriteria);

        jobEntity.setCommandCriterion(
            new CriterionEntity(this.createAndGetTagEntities(jobRequest.getCommandCriteria()))
        );
        jobEntity.setConfigs(this.createAndGetFileEntities(jobRequest.getConfigs()));
        jobEntity.setDependencies(this.createAndGetFileEntities(jobRequest.getDependencies()));
        jobEntity.setDisableLogArchival(jobRequest.isDisableLogArchival());
        jobRequest.getEmail().ifPresent(jobEntity::setEmail);
        if (!jobRequest.getTags().isEmpty()) {
            jobEntity.setTags(this.createAndGetTagEntities(jobRequest.getTags()));
        }
        jobRequest.getCpu().ifPresent(jobEntity::setCpuRequested);
        jobRequest.getMemory().ifPresent(jobEntity::setMemoryRequested);
        if (!jobRequest.getApplications().isEmpty()) {
            jobEntity.setApplicationsRequested(jobRequest.getApplications());
        }
        jobRequest.getTimeout().ifPresent(jobEntity::setTimeoutRequested);

        jobRequest.getGrouping().ifPresent(jobEntity::setGrouping);
        jobRequest.getGroupingInstance().ifPresent(jobEntity::setGroupingInstance);

        // Fields collected as metadata

        jobMetadata.getClientHost().ifPresent(jobEntity::setClientHost);
        jobMetadata.getUserAgent().ifPresent(jobEntity::setUserAgent);
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
        jobEntity.setHostName(jobExecution.getHostName());
        jobExecution.getProcessId().ifPresent(jobEntity::setProcessId);
        jobExecution.getCheckDelay().ifPresent(jobEntity::setCheckDelay);
        jobExecution.getTimeout().ifPresent(jobEntity::setTimeout);
        jobExecution.getMemory().ifPresent(jobEntity::setMemoryUsed);

        return jobEntity;
    }
}
