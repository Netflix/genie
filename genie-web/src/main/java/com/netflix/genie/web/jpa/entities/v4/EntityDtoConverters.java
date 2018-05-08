/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities.v4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest;
import com.netflix.genie.common.internal.dto.v4.AgentEnvironmentRequest;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.CriterionEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.projections.v4.V4JobRequestProjection;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Utility methods for converting from V4 DTO to entities and vice versa.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public final class EntityDtoConverters {

    private EntityDtoConverters() {
    }

    /**
     * Convert an application entity to a DTO for external exposure.
     *
     * @param applicationEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    public static Application toV4ApplicationDto(final ApplicationEntity applicationEntity) {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            applicationEntity.getName(),
            applicationEntity.getUser(),
            applicationEntity.getVersion(),
            applicationEntity.getStatus()
        )
            .withTags(applicationEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        applicationEntity.getType().ifPresent(metadataBuilder::withType);
        applicationEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        applicationEntity.getMetadata().ifPresent(metadata -> setJsonField(metadata, metadataBuilder::withMetadata));

        return new Application(
            applicationEntity.getUniqueId(),
            applicationEntity.getCreated(),
            applicationEntity.getUpdated(),
            new ExecutionEnvironment(
                applicationEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                applicationEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                applicationEntity.getSetupFile().isPresent() ? applicationEntity.getSetupFile().get().getFile() : null
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a cluster entity to a DTO for external exposure.
     *
     * @param clusterEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    public static Cluster toV4ClusterDto(final ClusterEntity clusterEntity) {
        final ClusterMetadata.Builder metadataBuilder = new ClusterMetadata.Builder(
            clusterEntity.getName(),
            clusterEntity.getUser(),
            clusterEntity.getVersion(),
            clusterEntity.getStatus()
        )
            .withTags(clusterEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        clusterEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        clusterEntity.getMetadata().ifPresent(metadata -> setJsonField(metadata, metadataBuilder::withMetadata));

        return new Cluster(
            clusterEntity.getUniqueId(),
            clusterEntity.getCreated(),
            clusterEntity.getUpdated(),
            new ExecutionEnvironment(
                clusterEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                clusterEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                clusterEntity.getSetupFile().isPresent() ? clusterEntity.getSetupFile().get().getFile() : null
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a command entity to a DTO for external exposure.
     *
     * @param commandEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     */
    public static Command toV4CommandDto(final CommandEntity commandEntity) {
        final CommandMetadata.Builder metadataBuilder = new CommandMetadata.Builder(
            commandEntity.getName(),
            commandEntity.getUser(),
            commandEntity.getVersion(),
            commandEntity.getStatus()
        )
            .withTags(commandEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        commandEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        commandEntity.getMetadata().ifPresent(metadata -> setJsonField(metadata, metadataBuilder::withMetadata));

        return new Command(
            commandEntity.getUniqueId(),
            commandEntity.getCreated(),
            commandEntity.getUpdated(),
            new ExecutionEnvironment(
                commandEntity.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                commandEntity.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
                commandEntity.getSetupFile().isPresent() ? commandEntity.getSetupFile().get().getFile() : null
            ),
            metadataBuilder.build(),
            commandEntity.getExecutable(),
            commandEntity.getMemory().orElse(null),
            commandEntity.getCheckDelay()
        );
    }

    /**
     * Convert a job request entity to a DTO.
     *
     * @param jobRequestProjection The projection of the {@link com.netflix.genie.web.jpa.entities.JobEntity} to
     *                             convert
     * @return The original job request DTO
     * @throws GeniePreconditionException When criterion can't be properly converted
     */
    public static JobRequest toV4JobRequestDto(
        final V4JobRequestProjection jobRequestProjection
    ) throws GeniePreconditionException {
        final String requestedId = jobRequestProjection.isRequestedId() ? jobRequestProjection.getUniqueId() : null;

        // Rebuild the job metadata
        final JobMetadata.Builder jobMetadataBuilder = new JobMetadata.Builder(
            jobRequestProjection.getName(),
            jobRequestProjection.getUser(),
            jobRequestProjection.getVersion()
        );
        jobRequestProjection.getGenieUserGroup().ifPresent(jobMetadataBuilder::withGroup);
        jobRequestProjection.getEmail().ifPresent(jobMetadataBuilder::withEmail);
        jobRequestProjection.getGrouping().ifPresent(jobMetadataBuilder::withGrouping);
        jobRequestProjection.getGroupingInstance().ifPresent(jobMetadataBuilder::withGroupingInstance);
        jobRequestProjection.getDescription().ifPresent(jobMetadataBuilder::withDescription);
        jobMetadataBuilder.withTags(
            jobRequestProjection.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet())
        );
        jobRequestProjection
            .getMetadata()
            .ifPresent(metadata -> setJsonField(metadata, jobMetadataBuilder::withMetadata));


        // Rebuild the execution resource criteria
        final ImmutableList.Builder<Criterion> clusterCriteria = ImmutableList.builder();
        for (final CriterionEntity criterionEntity : jobRequestProjection.getClusterCriteria()) {
            clusterCriteria.add(toCriterionDto(criterionEntity));
        }
        final Criterion commandCriterion = toCriterionDto(jobRequestProjection.getCommandCriterion());
        final ExecutionResourceCriteria executionResourceCriteria = new ExecutionResourceCriteria(
            clusterCriteria.build(),
            commandCriterion,
            jobRequestProjection.getRequestedApplications()
        );

        // Rebuild the job resources
        final String setupFile = jobRequestProjection.getSetupFile().map(FileEntity::getFile).orElse(null);
        final ExecutionEnvironment jobResources = new ExecutionEnvironment(
            jobRequestProjection.getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
            jobRequestProjection.getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet()),
            setupFile
        );

        // Rebuild the Agent Config Request
        final AgentConfigRequest.Builder agentConfigRequestBuilder = new AgentConfigRequest.Builder();
        jobRequestProjection
            .getRequestedAgentConfigExt()
            .ifPresent(ext -> setJsonField(ext, agentConfigRequestBuilder::withExt));
        jobRequestProjection
            .getRequestedJobDirectoryLocation()
            .ifPresent(agentConfigRequestBuilder::withRequestedJobDirectoryLocation);
        agentConfigRequestBuilder.withInteractive(jobRequestProjection.isInteractive());
        agentConfigRequestBuilder.withArchivingDisabled(jobRequestProjection.isArchivingDisabled());
        jobRequestProjection.getRequestedTimeout().ifPresent(agentConfigRequestBuilder::withTimeoutRequested);

        // Rebuild the Agent Environment Request
        final AgentEnvironmentRequest.Builder agentEnvironmentRequestBuilder = new AgentEnvironmentRequest.Builder();
        jobRequestProjection
            .getRequestedAgentEnvironmentExt()
            .ifPresent(ext -> setJsonField(ext, agentEnvironmentRequestBuilder::withExt));
        agentEnvironmentRequestBuilder
            .withRequestedEnvironmentVariables(jobRequestProjection.getRequestedEnvironmentVariables());
        jobRequestProjection.getRequestedCpu().ifPresent(agentEnvironmentRequestBuilder::withRequestedJobCpu);
        jobRequestProjection.getRequestedMemory().ifPresent(agentEnvironmentRequestBuilder::withRequestedJobMemory);

        return new JobRequest(
            requestedId,
            jobResources,
            jobRequestProjection.getCommandArgs(),
            jobMetadataBuilder.build(),
            executionResourceCriteria,
            agentEnvironmentRequestBuilder.build(),
            agentConfigRequestBuilder.build()
        );
    }

    private static Criterion toCriterionDto(final CriterionEntity criterionEntity) throws GeniePreconditionException {
        final Criterion.Builder builder = new Criterion.Builder();
        criterionEntity.getUniqueId().ifPresent(builder::withId);
        criterionEntity.getName().ifPresent(builder::withName);
        criterionEntity.getVersion().ifPresent(builder::withVersion);
        criterionEntity.getStatus().ifPresent(builder::withStatus);
        builder.withTags(criterionEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));
        return builder.build();
    }

    /**
     * Write a JSON string into the consumer function as a JSON node.
     *
     * @param json     The JSON string to serialize into a JSON node.
     * @param consumer The consuming function of the JSON node.
     */
    static void setJsonField(final String json, final Consumer<JsonNode> consumer) {
        try {
            consumer.accept(GenieObjectMapper.getMapper().readTree(json));
        } catch (final IOException ioe) {
            // Should never happen as preconditions for all inputs would have been checked before persistence happened
            log.error("Reading JSON string {} into JSON node failed with error {}", json, ioe.getMessage(), ioe);
            // Could recurse here and pass error json back through this method but don't want infinite loop
            consumer.accept(null);
        }
    }

    /**
     * Given a JSON node convert it to a string representation and hand it to the consumer.
     *
     * @param json     The JSON node to convert. If null {@literal null} will be passed to the consumer
     * @param consumer The consumer function to call with the string representation
     */
    public static void setJsonField(@Nullable final JsonNode json, final Consumer<String> consumer) {
        if (json != null) {
            try {
                consumer.accept(GenieObjectMapper.getMapper().writeValueAsString(json));
            } catch (final JsonProcessingException jpe) {
                // Should never happen as the JSON was valid to get to a JSON node in the first place
                log.error("Unable to write JSON node {} as string due to {}", json, jpe.getMessage(), jpe);
                consumer.accept("{\"jsonProcessingException\": \"" + jpe.getMessage() + "\"}");
            }
        } else {
            // Here this method is used to update entities so sometimes we'll want to set the value to null if a user
            // is requesting we clear out a value via an update call
            consumer.accept(null);
        }
    }
}
