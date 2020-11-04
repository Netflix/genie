/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.data.services.impl.jpa.converters;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.internal.dtos.v4.converters.DtoConverters;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.UserJobResourcesAggregate;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobExecutionProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobMetadataProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobRequestProjection;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Converters between entities and V3 DTOs.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Slf4j
public final class EntityV3DtoConverters {

    private EntityV3DtoConverters() {
    }

    /**
     * Convert the data in this job projection into a job DTO for external exposure.
     *
     * @param jobProjection The data from the database
     * @return The job DTO representation
     */
    public static Job toJobDto(final JobProjection jobProjection) {
        final Job.Builder builder = new Job.Builder(
            jobProjection.getName(),
            jobProjection.getUser(),
            jobProjection.getVersion()
        )
            .withId(jobProjection.getUniqueId())
            .withCreated(jobProjection.getCreated())
            .withUpdated(jobProjection.getUpdated())
            .withTags(jobProjection.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .withStatus(DtoConverters.toV3JobStatus(DtoConverters.toV4JobStatus(jobProjection.getStatus())))
            .withCommandArgs(jobProjection.getCommandArgs());

        jobProjection.getDescription().ifPresent(builder::withDescription);
        jobProjection.getStatusMsg().ifPresent(builder::withStatusMsg);
        jobProjection.getStarted().ifPresent(builder::withStarted);
        jobProjection.getFinished().ifPresent(builder::withFinished);
        jobProjection.getArchiveLocation().ifPresent(builder::withArchiveLocation);
        jobProjection.getClusterName().ifPresent(builder::withClusterName);
        jobProjection.getCommandName().ifPresent(builder::withCommandName);
        jobProjection.getGrouping().ifPresent(builder::withGrouping);
        jobProjection.getGroupingInstance().ifPresent(builder::withGroupingInstance);
        jobProjection.getMetadata().ifPresent(builder::withMetadata);

        return builder.build();
    }

    /**
     * Convert database record into a DTO.
     *
     * @param jobRequestProjection The database data to convert
     * @return A {@link JobRequest} instance
     */
    public static JobRequest toJobRequestDto(final JobRequestProjection jobRequestProjection) {
        final JobRequest.Builder builder = new JobRequest.Builder(
            jobRequestProjection.getName(),
            jobRequestProjection.getUser(),
            jobRequestProjection.getVersion(),
            jobRequestProjection
                .getClusterCriteria()
                .stream()
                .map(EntityV4DtoConverters::toCriterionDto)
                .map(DtoConverters::toClusterCriteria)
                .collect(Collectors.toList()),
            DtoConverters.toV3CriterionTags(
                EntityV4DtoConverters.toCriterionDto(jobRequestProjection.getCommandCriterion())
            )
        )
            .withCreated(jobRequestProjection.getCreated())
            .withId(jobRequestProjection.getUniqueId())
            .withDisableLogArchival(jobRequestProjection.isArchivingDisabled())
            .withConfigs(
                jobRequestProjection
                    .getConfigs()
                    .stream()
                    .map(FileEntity::getFile)
                    .collect(Collectors.toSet())
            )
            .withDependencies(
                jobRequestProjection
                    .getDependencies()
                    .stream()
                    .map(FileEntity::getFile)
                    .collect(Collectors.toSet())
            )
            .withTags(jobRequestProjection.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .withUpdated(jobRequestProjection.getUpdated())
            .withApplications(jobRequestProjection.getRequestedApplications())
            .withCommandArgs(jobRequestProjection.getCommandArgs());

        jobRequestProjection.getEmail().ifPresent(builder::withEmail);
        jobRequestProjection.getGenieUserGroup().ifPresent(builder::withGroup);
        jobRequestProjection.getDescription().ifPresent(builder::withDescription);
        jobRequestProjection.getRequestedCpu().ifPresent(builder::withCpu);
        jobRequestProjection.getRequestedMemory().ifPresent(builder::withMemory);
        jobRequestProjection.getRequestedTimeout().ifPresent(builder::withTimeout);
        jobRequestProjection
            .getSetupFile()
            .ifPresent(setupFileEntity -> builder.withSetupFile(setupFileEntity.getFile()));
        jobRequestProjection.getGrouping().ifPresent(builder::withGrouping);
        jobRequestProjection.getGroupingInstance().ifPresent(builder::withGroupingInstance);
        jobRequestProjection.getMetadata().ifPresent(builder::withMetadata);

        return builder.build();
    }

    /**
     * Convert job execution database data into a DTO.
     *
     * @param jobExecutionProjection The database data
     * @return {@link JobExecution} instance
     */
    public static JobExecution toJobExecutionDto(final JobExecutionProjection jobExecutionProjection) {
        final JobExecution.Builder builder = new JobExecution
            .Builder(jobExecutionProjection.getAgentHostname().orElse(UUID.randomUUID().toString()))
            .withId(jobExecutionProjection.getUniqueId())
            .withCreated(jobExecutionProjection.getCreated())
            .withUpdated(jobExecutionProjection.getUpdated());

        jobExecutionProjection.getProcessId().ifPresent(builder::withProcessId);
        jobExecutionProjection.getCheckDelay().ifPresent(builder::withCheckDelay);
        // Calculate the timeout as it used to be represented pre-timeoutUsed
        if (jobExecutionProjection.getStarted().isPresent() && jobExecutionProjection.getTimeoutUsed().isPresent()) {
            final Instant started = jobExecutionProjection.getStarted().get();
            builder.withTimeout(started.plusSeconds(jobExecutionProjection.getTimeoutUsed().get()));
        }
        jobExecutionProjection.getExitCode().ifPresent(builder::withExitCode);
        jobExecutionProjection.getMemoryUsed().ifPresent(builder::withMemory);
        try {
            builder.withArchiveStatus(
                ArchiveStatus.valueOf(
                    jobExecutionProjection.getArchiveStatus().orElseThrow(IllegalArgumentException::new)
                )
            );
        } catch (IllegalArgumentException e) {
            // If NULL or set to an invalid enum value
            builder.withArchiveStatus(ArchiveStatus.UNKNOWN);
        }
        jobExecutionProjection.getLauncherExt().ifPresent(builder::withLauncherExt);

        return builder.build();
    }

    /**
     * Convert job metadata information to a DTO.
     *
     * @param jobMetadataProjection The database information
     * @return A {@link JobMetadata} instance
     */
    public static JobMetadata toJobMetadataDto(final JobMetadataProjection jobMetadataProjection) {
        final JobMetadata.Builder builder = new JobMetadata.Builder()
            .withId(jobMetadataProjection.getUniqueId())
            .withCreated(jobMetadataProjection.getCreated())
            .withUpdated(jobMetadataProjection.getUpdated());

        jobMetadataProjection.getRequestApiClientHostname().ifPresent(builder::withClientHost);
        jobMetadataProjection.getRequestApiClientUserAgent().ifPresent(builder::withUserAgent);
        jobMetadataProjection.getNumAttachments().ifPresent(builder::withNumAttachments);
        jobMetadataProjection.getTotalSizeOfAttachments().ifPresent(builder::withTotalSizeOfAttachments);
        jobMetadataProjection.getStdErrSize().ifPresent(builder::withStdErrSize);
        jobMetadataProjection.getStdOutSize().ifPresent(builder::withStdOutSize);

        return builder.build();
    }

    /**
     * Convert a user resources summary to a DTO.
     *
     * @param userJobResourcesAggregate The database data to convert
     * @return A {@link UserResourcesSummary} instance
     */
    public static UserResourcesSummary toUserResourceSummaryDto(
        final UserJobResourcesAggregate userJobResourcesAggregate
    ) {
        final String user = userJobResourcesAggregate.getUser();
        final Long jobCount = userJobResourcesAggregate.getRunningJobsCount();
        final Long memory = userJobResourcesAggregate.getUsedMemory();
        return new UserResourcesSummary(
            user == null ? "NULL" : user,
            jobCount == null ? 0 : jobCount,
            memory == null ? 0 : memory
        );
    }
}
