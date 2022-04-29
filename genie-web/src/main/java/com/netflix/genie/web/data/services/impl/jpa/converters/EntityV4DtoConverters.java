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
package com.netflix.genie.web.data.services.impl.jpa.converters;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.AgentConfigRequest;
import com.netflix.genie.common.internal.dtos.Application;
import com.netflix.genie.common.internal.dtos.ApplicationMetadata;
import com.netflix.genie.common.internal.dtos.Cluster;
import com.netflix.genie.common.internal.dtos.ClusterMetadata;
import com.netflix.genie.common.internal.dtos.Command;
import com.netflix.genie.common.internal.dtos.CommandMetadata;
import com.netflix.genie.common.internal.dtos.ComputeResources;
import com.netflix.genie.common.internal.dtos.Criterion;
import com.netflix.genie.common.internal.dtos.ExecutionEnvironment;
import com.netflix.genie.common.internal.dtos.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dtos.FinishedJob;
import com.netflix.genie.common.internal.dtos.Image;
import com.netflix.genie.common.internal.dtos.JobEnvironmentRequest;
import com.netflix.genie.common.internal.dtos.JobMetadata;
import com.netflix.genie.common.internal.dtos.JobRequest;
import com.netflix.genie.common.internal.dtos.JobSpecification;
import com.netflix.genie.common.internal.dtos.converters.DtoConverters;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.web.data.services.impl.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CriterionEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.FinishedJobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.JobRequestProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.JobSpecificationProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility methods for converting from V4 DTO to entities and vice versa.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class EntityV4DtoConverters {

    private static final Logger LOG = LoggerFactory.getLogger(EntityV4DtoConverters.class);

    private static final TypeReference<Map<String, Image>> IMAGES_TYPE_REFERENCE = new TypeReference<>() {
    };
    private static final ObjectNode EMPTY_JSON = GenieObjectMapper.getMapper().createObjectNode();

    private EntityV4DtoConverters() {
    }

    /**
     * Convert an application entity to a DTO for external exposure.
     *
     * @param applicationEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     * @throws IllegalArgumentException On invalid field
     */
    public static Application toV4ApplicationDto(
        final ApplicationEntity applicationEntity
    ) throws IllegalArgumentException {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            applicationEntity.getName(),
            applicationEntity.getUser(),
            applicationEntity.getVersion(),
            DtoConverters.toV4ApplicationStatus(applicationEntity.getStatus())
        )
            .withTags(applicationEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        applicationEntity.getType().ifPresent(metadataBuilder::withType);
        applicationEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        applicationEntity.getMetadata().ifPresent(metadataBuilder::withMetadata);

        return new Application(
            applicationEntity.getUniqueId(),
            applicationEntity.getCreated(),
            applicationEntity.getUpdated(),
            toExecutionEnvironment(
                applicationEntity.getConfigs(),
                applicationEntity.getDependencies(),
                applicationEntity.getSetupFile().orElse(null)
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a cluster entity to a DTO for external exposure.
     *
     * @param clusterEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     * @throws IllegalArgumentException On any invalid field value
     */
    public static Cluster toV4ClusterDto(final ClusterEntity clusterEntity) throws IllegalArgumentException {
        final ClusterMetadata.Builder metadataBuilder = new ClusterMetadata.Builder(
            clusterEntity.getName(),
            clusterEntity.getUser(),
            clusterEntity.getVersion(),
            DtoConverters.toV4ClusterStatus(clusterEntity.getStatus())
        )
            .withTags(clusterEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        clusterEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        clusterEntity.getMetadata().ifPresent(metadataBuilder::withMetadata);

        return new Cluster(
            clusterEntity.getUniqueId(),
            clusterEntity.getCreated(),
            clusterEntity.getUpdated(),
            toExecutionEnvironment(
                clusterEntity.getConfigs(),
                clusterEntity.getDependencies(),
                clusterEntity.getSetupFile().orElse(null)
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a command entity to a DTO for external exposure.
     *
     * @param commandEntity The entity to convert
     * @return The immutable DTO representation of the entity data
     * @throws IllegalArgumentException On any invalid field value
     */
    public static Command toV4CommandDto(final CommandEntity commandEntity) throws IllegalArgumentException {
        final CommandMetadata.Builder metadataBuilder = new CommandMetadata.Builder(
            commandEntity.getName(),
            commandEntity.getUser(),
            commandEntity.getVersion(),
            DtoConverters.toV4CommandStatus(commandEntity.getStatus())
        )
            .withTags(commandEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));

        commandEntity.getDescription().ifPresent(metadataBuilder::withDescription);
        commandEntity.getMetadata().ifPresent(metadataBuilder::withMetadata);

        return new Command(
            commandEntity.getUniqueId(),
            commandEntity.getCreated(),
            commandEntity.getUpdated(),
            toExecutionEnvironment(
                commandEntity.getConfigs(),
                commandEntity.getDependencies(),
                commandEntity.getSetupFile().orElse(null)
            ),
            metadataBuilder.build(),
            commandEntity.getExecutable(),
            commandEntity
                .getClusterCriteria()
                .stream()
                .map(EntityV4DtoConverters::toCriterionDto)
                .collect(Collectors.toList()),
            toComputeResources(
                commandEntity::getCpu,
                commandEntity::getGpu,
                commandEntity::getMemory,
                commandEntity::getDiskMb,
                commandEntity::getNetworkMbps
            ),
            toImages(commandEntity::getImages)
        );
    }

    /**
     * Convert a job request entity to a DTO.
     *
     * @param jobRequestProjection The projection of the {@link JobRequestProjection} to convert
     * @return The original job request DTO
     * @throws GenieRuntimeException When criterion can't be properly converted
     */
    public static JobRequest toV4JobRequestDto(final JobRequestProjection jobRequestProjection) {
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
        jobRequestProjection.getMetadata().ifPresent(jobMetadataBuilder::withMetadata);

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
        final ExecutionEnvironment jobResources = toExecutionEnvironment(
            jobRequestProjection.getConfigs(),
            jobRequestProjection.getDependencies(),
            jobRequestProjection.getSetupFile().orElse(null)
        );

        // Rebuild the Agent Config Request
        final AgentConfigRequest.Builder agentConfigRequestBuilder = new AgentConfigRequest.Builder();
        jobRequestProjection.getRequestedAgentConfigExt().ifPresent(agentConfigRequestBuilder::withExt);
        jobRequestProjection
            .getRequestedJobDirectoryLocation()
            .ifPresent(agentConfigRequestBuilder::withRequestedJobDirectoryLocation);
        agentConfigRequestBuilder.withInteractive(jobRequestProjection.isInteractive());
        agentConfigRequestBuilder.withArchivingDisabled(jobRequestProjection.isArchivingDisabled());
        jobRequestProjection.getRequestedTimeout().ifPresent(agentConfigRequestBuilder::withTimeoutRequested);

        // Rebuild the Agent Environment Request
        final JobEnvironmentRequest.Builder jobEnvironmentRequestBuilder = new JobEnvironmentRequest.Builder();
        jobRequestProjection.getRequestedAgentEnvironmentExt().ifPresent(jobEnvironmentRequestBuilder::withExt);
        jobEnvironmentRequestBuilder
            .withRequestedEnvironmentVariables(jobRequestProjection.getRequestedEnvironmentVariables());
        jobEnvironmentRequestBuilder.withRequestedComputeResources(
            toComputeResources(
                jobRequestProjection::getRequestedCpu,
                jobRequestProjection::getRequestedGpu,
                jobRequestProjection::getRequestedMemory,
                jobRequestProjection::getRequestedDiskMb,
                jobRequestProjection::getRequestedNetworkMbps
            )
        );
        jobEnvironmentRequestBuilder.withRequestedImages(toImages(jobRequestProjection::getRequestedImages));

        return new JobRequest(
            requestedId,
            jobResources,
            jobRequestProjection.getCommandArgs(),
            jobMetadataBuilder.build(),
            executionResourceCriteria,
            jobEnvironmentRequestBuilder.build(),
            agentConfigRequestBuilder.build()
        );
    }

    /**
     * Convert a given {@link CriterionEntity} to the equivalent {@link Criterion} DTO representation.
     *
     * @param criterionEntity The entity to convert
     * @return The DTO representation
     */
    public static Criterion toCriterionDto(final CriterionEntity criterionEntity) {
        final Criterion.Builder builder = new Criterion.Builder();
        criterionEntity.getUniqueId().ifPresent(builder::withId);
        criterionEntity.getName().ifPresent(builder::withName);
        criterionEntity.getVersion().ifPresent(builder::withVersion);
        criterionEntity.getStatus().ifPresent(builder::withStatus);
        builder.withTags(criterionEntity.getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()));
        // Since these entities have been stored successfully they were validated before and shouldn't contain errors
        // we can't recover from error anyway in a good way here. Hence re-wrapping checked exception in runtime
        try {
            return builder.build();
        } catch (final IllegalArgumentException iae) {
            LOG.error("Creating a Criterion DTO from a Criterion entity threw exception", iae);
            // TODO: For now this is a generic GenieRuntimeException. If we would like more advanced logic at the
            //       edges (REST API, RPC API) based on type of exceptions we should subclass GenieRuntimeException
            throw new GenieRuntimeException(iae);
        }
    }

    /**
     * Convert a given {@link FinishedJobProjection} to the equivalent {@link FinishedJob} DTO representation.
     *
     * @param finishedJobProjection the entity projection
     * @return the DTO representation
     * @throws IllegalArgumentException On any invalid field
     */
    public static FinishedJob toFinishedJobDto(
        final FinishedJobProjection finishedJobProjection
    ) throws IllegalArgumentException {
        final FinishedJob.Builder builder = new FinishedJob.Builder(
            finishedJobProjection.getUniqueId(),
            finishedJobProjection.getName(),
            finishedJobProjection.getUser(),
            finishedJobProjection.getVersion(),
            finishedJobProjection.getCreated(),
            DtoConverters.toV4JobStatus(finishedJobProjection.getStatus()),
            finishedJobProjection.getCommandArgs(),
            toCriterionDto(finishedJobProjection.getCommandCriterion()),
            finishedJobProjection.getClusterCriteria()
                .stream()
                .map(EntityV4DtoConverters::toCriterionDto)
                .collect(Collectors.toList())
        );

        finishedJobProjection.getStarted().ifPresent(builder::withStarted);
        finishedJobProjection.getFinished().ifPresent(builder::withFinished);
        finishedJobProjection.getDescription().ifPresent(builder::withDescription);
        finishedJobProjection.getGrouping().ifPresent(builder::withGrouping);
        finishedJobProjection.getGroupingInstance().ifPresent(builder::withGroupingInstance);
        finishedJobProjection.getStatusMsg().ifPresent(builder::withStatusMessage);
        finishedJobProjection.getRequestedMemory().ifPresent(builder::withRequestedMemory);
        finishedJobProjection.getRequestApiClientHostname().ifPresent(builder::withRequestApiClientHostname);
        finishedJobProjection.getRequestApiClientUserAgent().ifPresent(builder::withRequestApiClientUserAgent);
        finishedJobProjection.getRequestAgentClientHostname().ifPresent(builder::withRequestAgentClientHostname);
        finishedJobProjection.getRequestAgentClientVersion().ifPresent(builder::withRequestAgentClientVersion);
        finishedJobProjection.getNumAttachments().ifPresent(builder::withNumAttachments);
        finishedJobProjection.getExitCode().ifPresent(builder::withExitCode);
        finishedJobProjection.getArchiveLocation().ifPresent(builder::withArchiveLocation);
        finishedJobProjection.getMemoryUsed().ifPresent(builder::withMemoryUsed);

        builder.withTags(
            finishedJobProjection.getTags()
                .stream()
                .map(TagEntity::getTag)
                .collect(Collectors.toSet())
        );

        finishedJobProjection.getMetadata().ifPresent(builder::withMetadata);

        finishedJobProjection.getCommand().ifPresent(
            commandEntity ->
                builder.withCommand(toV4CommandDto(commandEntity))
        );

        finishedJobProjection.getCluster().ifPresent(
            clusterEntity ->
                builder.withCluster(toV4ClusterDto(clusterEntity))
        );

        builder.withApplications(
            finishedJobProjection.getApplications()
                .stream()
                .map(EntityV4DtoConverters::toV4ApplicationDto)
                .collect(Collectors.toList())
        );

        return builder.build();
    }

    /**
     * Convert the values contained in the {@link JobSpecificationProjection} to an immutable {@link JobSpecification}
     * DTO.
     *
     * @param jobSpecificationProjection The entity values to convert
     * @return An immutable Job Specification instance
     * @throws GenieClusterNotFoundException When the cluster isn't found in the database which it should be at this
     *                                       point given the input to the db was valid at the time of persistence
     * @throws GenieCommandNotFoundException When the command isn't found in the database which it should be at this
     *                                       point given the input to the db was valid at the time of persistence
     * @throws GenieRuntimeException         All input should be valid at this point so if we can't create a job spec
     *                                       dto something has become corrupted in the db
     */
    public static JobSpecification toJobSpecificationDto(final JobSpecificationProjection jobSpecificationProjection) {
        final String id = jobSpecificationProjection.getUniqueId();

        // Check error conditions up front
        final ClusterEntity clusterEntity = jobSpecificationProjection
            .getCluster()
            .orElseThrow(
                () -> new GenieClusterNotFoundException("No cluster found for job " + id + ". Was expected to exist.")
            );
        final CommandEntity commandEntity = jobSpecificationProjection
            .getCommand()
            .orElseThrow(
                () -> new GenieCommandNotFoundException("No command found for job " + id + ". Was expected to exist.")
            );

        final File jobDirectoryLocation = jobSpecificationProjection
            .getJobDirectoryLocation()
            .map(File::new)
            .orElseThrow(
                () -> new GenieRuntimeException(
                    "No job directory location available for job " + id + ". Was expected to exist"
                )
            );

        final String archiveLocation = jobSpecificationProjection.getArchiveLocation().orElse(null);

        final JobSpecification.ExecutionResource job = toExecutionResource(
            id,
            jobSpecificationProjection.getConfigs(),
            jobSpecificationProjection.getDependencies(),
            jobSpecificationProjection.getSetupFile().orElse(null)
        );
        final JobSpecification.ExecutionResource cluster = toExecutionResource(
            clusterEntity.getUniqueId(),
            clusterEntity.getConfigs(),
            clusterEntity.getDependencies(),
            clusterEntity.getSetupFile().orElse(null)
        );
        final JobSpecification.ExecutionResource command = toExecutionResource(
            commandEntity.getUniqueId(),
            commandEntity.getConfigs(),
            commandEntity.getDependencies(),
            commandEntity.getSetupFile().orElse(null)
        );
        final List<JobSpecification.ExecutionResource> applications = jobSpecificationProjection
            .getApplications()
            .stream()
            .map(
                applicationEntity -> toExecutionResource(
                    applicationEntity.getUniqueId(),
                    applicationEntity.getConfigs(),
                    applicationEntity.getDependencies(),
                    applicationEntity.getSetupFile().orElse(null)
                )
            )
            .collect(Collectors.toList());

        return new JobSpecification(
            commandEntity.getExecutable(),
            jobSpecificationProjection.getCommandArgs(),
            job,
            cluster,
            command,
            applications,
            jobSpecificationProjection.getEnvironmentVariables(),
            jobSpecificationProjection.isInteractive(),
            jobDirectoryLocation,
            archiveLocation,
            jobSpecificationProjection.getTimeoutUsed().orElse(null)
        );
    }

    private static JobSpecification.ExecutionResource toExecutionResource(
        final String id,
        final Set<FileEntity> configs,
        final Set<FileEntity> dependencies,
        @Nullable final FileEntity setupFile
    ) {
        return new JobSpecification.ExecutionResource(id, toExecutionEnvironment(configs, dependencies, setupFile));
    }

    private static ExecutionEnvironment toExecutionEnvironment(
        final Set<FileEntity> configs,
        final Set<FileEntity> dependencies,
        @Nullable final FileEntity setupFile
    ) {
        return new ExecutionEnvironment(
            configs.stream().map(FileEntity::getFile).collect(Collectors.toSet()),
            dependencies.stream().map(FileEntity::getFile).collect(Collectors.toSet()),
            setupFile != null ? setupFile.getFile() : null
        );
    }

    private static ComputeResources toComputeResources(
        final Supplier<Optional<Integer>> cpuGetter,
        final Supplier<Optional<Integer>> gpuGetter,
        final Supplier<Optional<Long>> memoryGetter,
        final Supplier<Optional<Long>> diskMbGetter,
        final Supplier<Optional<Long>> networkMbpsGetter
    ) {
        return new ComputeResources
            .Builder()
            .withCpu(cpuGetter.get().orElse(null))
            .withGpu(gpuGetter.get().orElse(null))
            .withMemoryMb(memoryGetter.get().orElse(null))
            .withDiskMb(diskMbGetter.get().orElse(null))
            .withNetworkMbps(networkMbpsGetter.get().orElse(null))
            .build();
    }

    private static Map<String, Image> toImages(final Supplier<Optional<JsonNode>> imagesGetter) {
        return GenieObjectMapper
            .getMapper()
            .convertValue(imagesGetter.get().orElse(EMPTY_JSON), IMAGES_TYPE_REFERENCE);
    }
}
