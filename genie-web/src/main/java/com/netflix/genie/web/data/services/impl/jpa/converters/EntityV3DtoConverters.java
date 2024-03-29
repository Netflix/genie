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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.genie.common.dto.ArchiveStatus;
import com.netflix.genie.common.dto.ContainerImage;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.Runtime;
import com.netflix.genie.common.dto.RuntimeResources;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.Image;
import com.netflix.genie.common.internal.dtos.converters.DtoConverters;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.UserJobResourcesAggregate;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobExecutionProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobMetadataProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobProjection;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
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
    private static final TypeReference<Map<String, Image>> IMAGES_TYPE_REFERENCE = new TypeReference<>() {
    };
    private static final ObjectNode EMPTY_JSON = GenieObjectMapper.getMapper().createObjectNode();

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
        // Calculate the timeout as it used to be represented pre-timeoutUsed
        if (jobExecutionProjection.getStarted().isPresent() && jobExecutionProjection.getTimeoutUsed().isPresent()) {
            final Instant started = jobExecutionProjection.getStarted().get();
            builder.withTimeout(started.plusSeconds(jobExecutionProjection.getTimeoutUsed().get()));
        }
        jobExecutionProjection.getExitCode().ifPresent(builder::withExitCode);
        final RuntimeResources.Builder resourcesBuilder = new RuntimeResources.Builder();
        jobExecutionProjection.getCpuUsed().ifPresent(resourcesBuilder::withCpu);
        jobExecutionProjection.getGpuUsed().ifPresent(resourcesBuilder::withGpu);
        jobExecutionProjection.getMemoryUsed().ifPresent(resourcesBuilder::withMemoryMb);
        jobExecutionProjection.getDiskMbUsed().ifPresent(resourcesBuilder::withDiskMb);
        jobExecutionProjection.getNetworkMbpsUsed().ifPresent(resourcesBuilder::withNetworkMbps);

        final Map<String, ContainerImage> images = GenieObjectMapper
            .getMapper()
            .convertValue(jobExecutionProjection.getImagesUsed().orElse(EMPTY_JSON), IMAGES_TYPE_REFERENCE)
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> DtoConverters.toV3ContainerImage(entry.getValue())
                )
            );
        builder.withRuntime(new Runtime.Builder().withResources(resourcesBuilder.build()).withImages(images).build());

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
