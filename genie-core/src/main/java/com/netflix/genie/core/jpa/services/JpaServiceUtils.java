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
package com.netflix.genie.core.jpa.services;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.CriterionEntity;
import com.netflix.genie.core.jpa.entities.FileEntity;
import com.netflix.genie.core.jpa.entities.TagEntity;
import com.netflix.genie.core.jpa.entities.projections.JobExecutionProjection;
import com.netflix.genie.core.jpa.entities.projections.JobMetadataProjection;
import com.netflix.genie.core.jpa.entities.projections.JobProjection;
import com.netflix.genie.core.jpa.entities.projections.JobRequestProjection;

import java.util.stream.Collectors;

/**
 * Utility methods for JPA services.
 *
 * @author tgianos
 * @since 3.3.0
 */
public class JpaServiceUtils {

    private JpaServiceUtils() {
    }

    /**
     * Convert an application entity to a DTO for external exposure.
     *
     * @param applicationEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    static Application toApplicationDto(final ApplicationEntity applicationEntity) {
        final Application.Builder builder = new Application.Builder(
            applicationEntity.getName(),
            applicationEntity.getUser(),
            applicationEntity.getVersion(),
            applicationEntity.getStatus()
        )
            .withId(applicationEntity.getUniqueId())
            .withCreated(applicationEntity.getCreated())
            .withUpdated(applicationEntity.getUpdated())
            .withTags(applicationEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .withConfigs(applicationEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()))
            .withDependencies(
                applicationEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet())
            );

        applicationEntity.getType().ifPresent(builder::withType);
        applicationEntity.getDescription().ifPresent(builder::withDescription);
        applicationEntity.getSetupFile().ifPresent(setupFileEntity -> builder.withSetupFile(setupFileEntity.getFile()));

        return builder.build();
    }

    /**
     * Convert a cluster entity to a DTO for external exposure.
     *
     * @param clusterEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    static Cluster toClusterDto(final ClusterEntity clusterEntity) {
        final Cluster.Builder builder = new Cluster.Builder(
            clusterEntity.getName(),
            clusterEntity.getUser(),
            clusterEntity.getVersion(),
            clusterEntity.getStatus()
        )
            .withId(clusterEntity.getUniqueId())
            .withCreated(clusterEntity.getCreated())
            .withUpdated(clusterEntity.getUpdated())
            .withTags(clusterEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .withConfigs(clusterEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()))
            .withDependencies(
                clusterEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet())
            );

        clusterEntity.getDescription().ifPresent(builder::withDescription);
        clusterEntity.getSetupFile().ifPresent(setupFileEntity -> builder.withSetupFile(setupFileEntity.getFile()));

        return builder.build();
    }

    /**
     * Convert a command entity to a DTO for external exposure.
     *
     * @param commandEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    static Command toCommandDto(final CommandEntity commandEntity) {
        final Command.Builder builder = new Command.Builder(
            commandEntity.getName(),
            commandEntity.getUser(),
            commandEntity.getVersion(),
            commandEntity.getStatus(),
            commandEntity.getExecutable(),
            commandEntity.getCheckDelay()
        )
            .withId(commandEntity.getUniqueId())
            .withCreated(commandEntity.getCreated())
            .withUpdated(commandEntity.getUpdated())
            .withTags(commandEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .withConfigs(commandEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()))
            .withDependencies(
                commandEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet())
            );

        commandEntity.getMemory().ifPresent(builder::withMemory);
        commandEntity.getDescription().ifPresent(builder::withDescription);
        commandEntity.getSetupFile().ifPresent(setupFileEntity -> builder.withSetupFile(setupFileEntity.getFile()));

        return builder.build();
    }

    /**
     * Convert the data in this job projection into a job DTO
     *
     * @return The job DTO representation
     */
    static Job toJobDto(final JobProjection jobProjection) {
        final Job.Builder builder = new Job.Builder(
            jobProjection.getName(),
            jobProjection.getUser(),
            jobProjection.getVersion(),
            jobProjection.getCommandArgs().orElse(null)
        )
            .withId(jobProjection.getUniqueId())
            .withCreated(jobProjection.getCreated())
            .withUpdated(jobProjection.getUpdated())
            .withTags(jobProjection.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .withStatus(jobProjection.getStatus());

        jobProjection.getDescription().ifPresent(builder::withDescription);
        jobProjection.getStatusMsg().ifPresent(builder::withStatusMsg);
        jobProjection.getStarted().ifPresent(builder::withStarted);
        jobProjection.getFinished().ifPresent(builder::withFinished);
        jobProjection.getArchiveLocation().ifPresent(builder::withArchiveLocation);
        jobProjection.getClusterName().ifPresent(builder::withClusterName);
        jobProjection.getCommandName().ifPresent(builder::withCommandName);

        return builder.build();
    }

    static JobRequest toJobRequestDto(final JobRequestProjection jobRequestProjection) {
        final JobRequest.Builder builder = new JobRequest.Builder(
            jobRequestProjection.getName(),
            jobRequestProjection.getUser(),
            jobRequestProjection.getVersion(),
            jobRequestProjection.getCommandArgs().orElse(null),
            jobRequestProjection
                .getClusterCriteria()
                .stream()
                .map(JpaServiceUtils::toClusterCriteriaDto)
                .collect(Collectors.toList()),
            jobRequestProjection
                .getCommandCriterion()
                .orElse(new CriterionEntity())
                .getTags()
                .stream()
                .map(TagEntity::getTag)
                .collect(Collectors.toSet())
        )
            .withCreated(jobRequestProjection.getCreated())
            .withId(jobRequestProjection.getUniqueId())
            .withDisableLogArchival(jobRequestProjection.isDisableLogArchival())
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
            .withApplications(jobRequestProjection.getApplicationsRequested())
            .withTimeout(jobRequestProjection.getTimeoutRequested().orElse(null));

        jobRequestProjection.getEmail().ifPresent(builder::withEmail);
        jobRequestProjection.getGenieUserGroup().ifPresent(builder::withGroup);
        jobRequestProjection.getDescription().ifPresent(builder::withDescription);
        jobRequestProjection.getCpuRequested().ifPresent(builder::withCpu);
        jobRequestProjection.getMemoryRequested().ifPresent(builder::withMemory);
        jobRequestProjection.getTimeoutRequested().ifPresent(builder::withTimeout);
        jobRequestProjection
            .getSetupFile()
            .ifPresent(setupFileEntity -> builder.withSetupFile(setupFileEntity.getFile()));

        return builder.build();
    }

    private static ClusterCriteria toClusterCriteriaDto(final CriterionEntity clusterCriterionEntity) {
        return new ClusterCriteria(
            clusterCriterionEntity
                .getTags()
                .stream()
                .map(TagEntity::getTag)
                .collect(Collectors.toSet())
        );
    }

    static JobExecution toJobExecutionDto(final JobExecutionProjection jobExecutionProjection) {
        final JobExecution.Builder builder = new JobExecution.Builder(jobExecutionProjection.getHostName())
            .withId(jobExecutionProjection.getUniqueId())
            .withCreated(jobExecutionProjection.getCreated())
            .withUpdated(jobExecutionProjection.getUpdated());

        jobExecutionProjection.getProcessId().ifPresent(builder::withProcessId);
        jobExecutionProjection.getCheckDelay().ifPresent(builder::withCheckDelay);
        jobExecutionProjection.getTimeout().ifPresent(builder::withTimeout);
        jobExecutionProjection.getExitCode().ifPresent(builder::withExitCode);
        jobExecutionProjection.getMemoryUsed().ifPresent(builder::withMemory);

        return builder.build();
    }

    static JobMetadata toJobMetadataDto(final JobMetadataProjection jobMetadataProjection) {
        final JobMetadata.Builder builder = new JobMetadata.Builder()
            .withId(jobMetadataProjection.getUniqueId())
            .withCreated(jobMetadataProjection.getCreated())
            .withUpdated(jobMetadataProjection.getUpdated());

        jobMetadataProjection.getClientHost().ifPresent(builder::withClientHost);
        jobMetadataProjection.getUserAgent().ifPresent(builder::withUserAgent);
        jobMetadataProjection.getNumAttachments().ifPresent(builder::withNumAttachments);
        jobMetadataProjection.getTotalSizeOfAttachments().ifPresent(builder::withTotalSizeOfAttachments);
        jobMetadataProjection.getStdErrSize().ifPresent(builder::withStdErrSize);
        jobMetadataProjection.getStdOutSize().ifPresent(builder::withStdOutSize);

        return builder.build();
    }
}
