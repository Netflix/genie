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
package com.netflix.genie.common.internal.dtos.v4.converters;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.ApplicationMetadata;
import com.netflix.genie.common.external.dtos.v4.ApplicationRequest;
import com.netflix.genie.common.external.dtos.v4.ApplicationStatus;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata;
import com.netflix.genie.common.external.dtos.v4.ClusterRequest;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandRequest;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobArchivalDataRequest;
import com.netflix.genie.common.external.dtos.v4.JobEnvironmentRequest;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.util.JsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class to help converting between V3 and V4 DTOs during migration period.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class DtoConverters {

    /**
     * The Genie 3 prefix for resource ID added to the set of tags by the system.
     */
    public static final String GENIE_ID_PREFIX = "genie.id:";
    /**
     * The Genie 3 prefix for resource names added to the set of tags by the system.
     */
    public static final String GENIE_NAME_PREFIX = "genie.name:";

    private DtoConverters() {
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.Application} to a corresponding V4 {@link ApplicationRequest}.
     *
     * @param v3Application The application to convert
     * @return An immutable {@link ApplicationRequest} instance
     * @throws IllegalArgumentException If any field is invalid during the conversion
     */
    public static ApplicationRequest toV4ApplicationRequest(
        final com.netflix.genie.common.dto.Application v3Application
    ) throws IllegalArgumentException {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            v3Application.getName(),
            v3Application.getUser(),
            v3Application.getVersion(),
            toV4ApplicationStatus(v3Application.getStatus())
        )
            .withTags(toV4Tags(v3Application.getTags()));

        v3Application.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3Application.getType().ifPresent(metadataBuilder::withType);
        v3Application.getDescription().ifPresent(metadataBuilder::withDescription);

        final ApplicationRequest.Builder builder = new ApplicationRequest.Builder(metadataBuilder.build());
        v3Application.getId().ifPresent(builder::withRequestedId);
        builder.withResources(
            new ExecutionEnvironment(
                v3Application.getConfigs(),
                v3Application.getDependencies(),
                v3Application.getSetupFile().orElse(null)
            )
        );

        return builder.build();
    }

    /**
     * Convert a V3 Application DTO to a V4 Application DTO.
     *
     * @param v3Application The V3 application to convert
     * @return The V4 application representation of the data in the V3 DTO
     * @throws IllegalArgumentException On invalid argument to one of the fields
     */
    public static Application toV4Application(
        final com.netflix.genie.common.dto.Application v3Application
    ) throws IllegalArgumentException {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            v3Application.getName(),
            v3Application.getUser(),
            v3Application.getVersion(),
            toV4ApplicationStatus(v3Application.getStatus())
        )
            .withTags(toV4Tags(v3Application.getTags()));

        v3Application.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3Application.getType().ifPresent(metadataBuilder::withType);
        v3Application.getDescription().ifPresent(metadataBuilder::withDescription);

        return new Application(
            v3Application.getId().orElseThrow(IllegalArgumentException::new),
            v3Application.getCreated().orElse(Instant.now()),
            v3Application.getUpdated().orElse(Instant.now()),
            new ExecutionEnvironment(
                v3Application.getConfigs(),
                v3Application.getDependencies(),
                v3Application.getSetupFile().orElse(null)
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a V4 Application DTO to a V3 application DTO.
     *
     * @param v4Application The V4 application to convert
     * @return The V3 application representation of the data in the V4 DTO
     * @throws IllegalArgumentException On invalid argument for one of the fields
     */
    public static com.netflix.genie.common.dto.Application toV3Application(
        final Application v4Application
    ) throws IllegalArgumentException {
        final ApplicationMetadata applicationMetadata = v4Application.getMetadata();
        final ExecutionEnvironment resources = v4Application.getResources();
        final com.netflix.genie.common.dto.Application.Builder builder
            = new com.netflix.genie.common.dto.Application.Builder(
            applicationMetadata.getName(),
            applicationMetadata.getUser(),
            applicationMetadata.getVersion(),
            toV3ApplicationStatus(applicationMetadata.getStatus())
        )
            .withId(v4Application.getId())
            .withTags(toV3Tags(v4Application.getId(), applicationMetadata.getName(), applicationMetadata.getTags()))
            .withConfigs(resources.getConfigs())
            .withDependencies(resources.getDependencies())
            .withCreated(v4Application.getCreated())
            .withUpdated(v4Application.getUpdated());

        applicationMetadata.getType().ifPresent(builder::withType);
        applicationMetadata.getDescription().ifPresent(builder::withDescription);
        applicationMetadata.getMetadata().ifPresent(builder::withMetadata);

        resources.getSetupFile().ifPresent(builder::withSetupFile);

        return builder.build();
    }

    /**
     * Convert a {@link com.netflix.genie.common.dto.Cluster} to a V4 {@link ClusterRequest}.
     *
     * @param v3Cluster The V3 cluster instance to convert
     * @return An immutable {@link ClusterRequest} instance
     * @throws IllegalArgumentException On any invalid field during conversion
     */
    public static ClusterRequest toV4ClusterRequest(final com.netflix.genie.common.dto.Cluster v3Cluster) {
        final ClusterMetadata.Builder metadataBuilder = new ClusterMetadata.Builder(
            v3Cluster.getName(),
            v3Cluster.getUser(),
            v3Cluster.getVersion(),
            toV4ClusterStatus(v3Cluster.getStatus())
        )
            .withTags(toV4Tags(v3Cluster.getTags()));

        v3Cluster.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3Cluster.getDescription().ifPresent(metadataBuilder::withDescription);

        final ClusterRequest.Builder builder = new ClusterRequest.Builder(metadataBuilder.build());
        v3Cluster.getId().ifPresent(builder::withRequestedId);
        builder.withResources(
            new ExecutionEnvironment(
                v3Cluster.getConfigs(),
                v3Cluster.getDependencies(),
                v3Cluster.getSetupFile().orElse(null)
            )
        );

        return builder.build();
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.Cluster} to a V4 {@link Cluster}.
     *
     * @param v3Cluster The cluster to convert
     * @return The V4 representation of the cluster
     * @throws IllegalArgumentException On any invalid field during conversion
     */
    public static Cluster toV4Cluster(
        final com.netflix.genie.common.dto.Cluster v3Cluster
    ) throws IllegalArgumentException {
        final ClusterMetadata.Builder metadataBuilder = new ClusterMetadata.Builder(
            v3Cluster.getName(),
            v3Cluster.getUser(),
            v3Cluster.getVersion(),
            toV4ClusterStatus(v3Cluster.getStatus())
        )
            .withTags(toV4Tags(v3Cluster.getTags()));

        v3Cluster.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3Cluster.getDescription().ifPresent(metadataBuilder::withDescription);

        return new Cluster(
            v3Cluster.getId().orElseThrow(IllegalArgumentException::new),
            v3Cluster.getCreated().orElse(Instant.now()),
            v3Cluster.getUpdated().orElse(Instant.now()),
            new ExecutionEnvironment(
                v3Cluster.getConfigs(),
                v3Cluster.getDependencies(),
                v3Cluster.getSetupFile().orElse(null)
            ),
            metadataBuilder.build()
        );
    }

    /**
     * Convert a V4 {@link Cluster} to a V3 {@link com.netflix.genie.common.dto.Cluster}.
     *
     * @param v4Cluster The cluster to convert
     * @return The v3 cluster
     * @throws IllegalArgumentException On any invalid field during conversion
     */
    public static com.netflix.genie.common.dto.Cluster toV3Cluster(
        final Cluster v4Cluster
    ) throws IllegalArgumentException {
        final ClusterMetadata clusterMetadata = v4Cluster.getMetadata();
        final ExecutionEnvironment resources = v4Cluster.getResources();
        final com.netflix.genie.common.dto.Cluster.Builder builder = new com.netflix.genie.common.dto.Cluster.Builder(
            clusterMetadata.getName(),
            clusterMetadata.getUser(),
            clusterMetadata.getVersion(),
            toV3ClusterStatus(clusterMetadata.getStatus())
        )
            .withId(v4Cluster.getId())
            .withTags(toV3Tags(v4Cluster.getId(), clusterMetadata.getName(), clusterMetadata.getTags()))
            .withConfigs(resources.getConfigs())
            .withDependencies(resources.getDependencies())
            .withCreated(v4Cluster.getCreated())
            .withUpdated(v4Cluster.getUpdated());

        clusterMetadata.getDescription().ifPresent(builder::withDescription);
        clusterMetadata.getMetadata().ifPresent(builder::withMetadata);

        resources.getSetupFile().ifPresent(builder::withSetupFile);

        return builder.build();
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.Command} to a V4 {@link CommandRequest}.
     *
     * @param v3Command The V3 command to convert
     * @return An immutable {@link CommandRequest} instance
     * @throws IllegalArgumentException On any invalid field during conversion
     */
    public static CommandRequest toV4CommandRequest(
        final com.netflix.genie.common.dto.Command v3Command
    ) throws IllegalArgumentException {
        final CommandMetadata.Builder metadataBuilder = new CommandMetadata.Builder(
            v3Command.getName(),
            v3Command.getUser(),
            v3Command.getVersion(),
            toV4CommandStatus(v3Command.getStatus())
        )
            .withTags(toV4Tags(v3Command.getTags()));

        v3Command.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3Command.getDescription().ifPresent(metadataBuilder::withDescription);

        final List<String> executable = v3Command.getExecutableAndArguments();
        final CommandRequest.Builder builder = new CommandRequest
            .Builder(metadataBuilder.build(), executable)
            .withCheckDelay(v3Command.getCheckDelay());
        v3Command.getId().ifPresent(builder::withRequestedId);
        v3Command.getMemory().ifPresent(builder::withMemory);
        builder.withResources(
            new ExecutionEnvironment(
                v3Command.getConfigs(),
                v3Command.getDependencies(),
                v3Command.getSetupFile().orElse(null)
            )
        );

        return builder.build();
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.Command} to a V4 {@link Command}.
     *
     * @param v3Command The V3 Command to convert
     * @return The V4 representation of the supplied command
     * @throws IllegalArgumentException On any invalid field during conversion
     */
    public static Command toV4Command(
        final com.netflix.genie.common.dto.Command v3Command
    ) throws IllegalArgumentException {
        final CommandMetadata.Builder metadataBuilder = new CommandMetadata.Builder(
            v3Command.getName(),
            v3Command.getUser(),
            v3Command.getVersion(),
            toV4CommandStatus(v3Command.getStatus())
        )
            .withTags(toV4Tags(v3Command.getTags()));

        v3Command.getDescription().ifPresent(metadataBuilder::withDescription);
        v3Command.getMetadata().ifPresent(metadataBuilder::withMetadata);

        final ExecutionEnvironment resources = new ExecutionEnvironment(
            v3Command.getConfigs(),
            v3Command.getDependencies(),
            v3Command.getSetupFile().orElse(null)
        );

        return new Command(
            v3Command.getId().orElseThrow(IllegalArgumentException::new),
            v3Command.getCreated().orElse(Instant.now()),
            v3Command.getUpdated().orElse(Instant.now()),
            resources,
            metadataBuilder.build(),
            v3Command.getExecutableAndArguments(),
            v3Command.getMemory().orElse(null),
            v3Command.getCheckDelay()
        );
    }

    /**
     * Convert a V4 {@link Command} to a V3 {@link com.netflix.genie.common.dto.Command}.
     *
     * @param v4Command The V4 command to convert
     * @return An immutable V3 Command instance
     * @throws IllegalArgumentException On any invalid field during conversion
     */
    public static com.netflix.genie.common.dto.Command toV3Command(
        final Command v4Command
    ) throws IllegalArgumentException {
        final CommandMetadata commandMetadata = v4Command.getMetadata();
        final ExecutionEnvironment resources = v4Command.getResources();
        final com.netflix.genie.common.dto.Command.Builder builder = new com.netflix.genie.common.dto.Command.Builder(
            commandMetadata.getName(),
            commandMetadata.getUser(),
            commandMetadata.getVersion(),
            toV3CommandStatus(commandMetadata.getStatus()),
            v4Command.getExecutable(),
            v4Command.getCheckDelay()
        )
            .withId(v4Command.getId())
            .withTags(toV3Tags(v4Command.getId(), commandMetadata.getName(), commandMetadata.getTags()))
            .withConfigs(resources.getConfigs())
            .withDependencies(resources.getDependencies())
            .withCreated(v4Command.getCreated())
            .withUpdated(v4Command.getUpdated());

        commandMetadata.getDescription().ifPresent(builder::withDescription);
        commandMetadata.getMetadata().ifPresent(builder::withMetadata);

        resources.getSetupFile().ifPresent(builder::withSetupFile);

        v4Command.getMemory().ifPresent(builder::withMemory);

        return builder.build();
    }

    /**
     * Convert a V3 Job Request to a V4 Job Request.
     *
     * @param v3JobRequest The v3 request to convert
     * @return The V4 version of the information contained in the V3 request
     * @throws GeniePreconditionException When the criteria is invalid
     */
    public static JobRequest toV4JobRequest(
        final com.netflix.genie.common.dto.JobRequest v3JobRequest
    ) throws GeniePreconditionException {
        return DtoConverters.toV4JobRequest(v3JobRequest, false);
    }

    /**
     * Convert a V3 Job Request to a V4 Job Request.
     *
     * @param v3JobRequest            The v3 request to convert
     * @param tokenizeArgumentsString Whether to perform splitting of the command arguments string into separate
     *                                arguments. This is necessary to execute a request that came through the V3 API
     *                                using V4 execution code.
     * @return The V4 version of the information contained in the V3 request
     * @throws GeniePreconditionException When the criteria is invalid
     */
    public static JobRequest toV4JobRequest(
        final com.netflix.genie.common.dto.JobRequest v3JobRequest,
        final boolean tokenizeArgumentsString
    ) throws GeniePreconditionException {
        final ExecutionEnvironment resources = new ExecutionEnvironment(
            v3JobRequest.getConfigs(),
            v3JobRequest.getDependencies(),
            v3JobRequest.getSetupFile().orElse(null)
        );

        final JobMetadata.Builder metadataBuilder = new JobMetadata.Builder(
            v3JobRequest.getName(),
            v3JobRequest.getUser(),
            v3JobRequest.getVersion()
        )
            .withTags(v3JobRequest.getTags());

        v3JobRequest.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3JobRequest.getEmail().ifPresent(metadataBuilder::withEmail);
        v3JobRequest.getGroup().ifPresent(metadataBuilder::withGroup);
        v3JobRequest.getGrouping().ifPresent(metadataBuilder::withGrouping);
        v3JobRequest.getGroupingInstance().ifPresent(metadataBuilder::withGroupingInstance);
        v3JobRequest.getDescription().ifPresent(metadataBuilder::withDescription);

        final List<Criterion> clusterCriteria = Lists.newArrayList();
        for (final ClusterCriteria criterion : v3JobRequest.getClusterCriterias()) {
            clusterCriteria.add(toV4Criterion(criterion));
        }
        final ExecutionResourceCriteria criteria = new ExecutionResourceCriteria(
            clusterCriteria,
            toV4Criterion(v3JobRequest.getCommandCriteria()),
            v3JobRequest.getApplications()
        );

        final JobEnvironmentRequest.Builder jobEnvironmentBuilder = new JobEnvironmentRequest.Builder();
        v3JobRequest.getCpu().ifPresent(jobEnvironmentBuilder::withRequestedJobCpu);
        v3JobRequest.getMemory().ifPresent(jobEnvironmentBuilder::withRequestedJobMemory);

        final AgentConfigRequest.Builder agentConfigBuilder = new AgentConfigRequest
            .Builder()
            .withArchivingDisabled(v3JobRequest.isDisableLogArchival())
            .withInteractive(false);
        v3JobRequest.getTimeout().ifPresent(agentConfigBuilder::withTimeoutRequested);

        final JobArchivalDataRequest.Builder jobArchivalDataRequestBuilder = new JobArchivalDataRequest.Builder();

        final List<String> commandArgs = Lists.newArrayList();
        if (tokenizeArgumentsString) {
            v3JobRequest.getCommandArgs().ifPresent(args -> commandArgs.addAll(JsonUtils.splitArguments(args)));
        } else {
            v3JobRequest.getCommandArgs().ifPresent(commandArgs::add);
        }

        return new JobRequest(
            v3JobRequest.getId().orElse(null),
            resources,
            commandArgs,
            metadataBuilder.build(),
            criteria,
            jobEnvironmentBuilder.build(),
            agentConfigBuilder.build(),
            jobArchivalDataRequestBuilder.build()
        );
    }

    /**
     * Helper method to convert a v4 JobRequest to a v3 job request.
     *
     * @param v4JobRequest The v4 job request instance
     * @return The v3 job request instance
     */
    public static com.netflix.genie.common.dto.JobRequest toV3JobRequest(final JobRequest v4JobRequest) {
        final com.netflix.genie.common.dto.JobRequest.Builder v3Builder
            = new com.netflix.genie.common.dto.JobRequest.Builder(
            v4JobRequest.getMetadata().getName(),
            v4JobRequest.getMetadata().getUser(),
            v4JobRequest.getMetadata().getVersion(),
            v4JobRequest
                .getCriteria()
                .getClusterCriteria()
                .stream()
                .map(DtoConverters::toClusterCriteria)
                .collect(Collectors.toList()),
            toV3CriterionTags(v4JobRequest.getCriteria().getCommandCriterion())
        )
            .withApplications(v4JobRequest.getCriteria().getApplicationIds())
            .withCommandArgs(v4JobRequest.getCommandArgs())
            .withDisableLogArchival(v4JobRequest.getRequestedAgentConfig().isArchivingDisabled())
            .withTags(v4JobRequest.getMetadata().getTags());

        v4JobRequest.getRequestedId().ifPresent(v3Builder::withId);

        final JobMetadata metadata = v4JobRequest.getMetadata();
        metadata.getEmail().ifPresent(v3Builder::withEmail);
        metadata.getGroup().ifPresent(v3Builder::withGroup);
        metadata.getGrouping().ifPresent(v3Builder::withGrouping);
        metadata.getGroupingInstance().ifPresent(v3Builder::withGroupingInstance);
        metadata.getDescription().ifPresent(v3Builder::withDescription);
        metadata.getMetadata().ifPresent(v3Builder::withMetadata);

        final ExecutionEnvironment jobResources = v4JobRequest.getResources();
        v3Builder.withConfigs(jobResources.getConfigs());
        v3Builder.withDependencies(jobResources.getDependencies());
        jobResources.getSetupFile().ifPresent(v3Builder::withSetupFile);

        v4JobRequest.getRequestedAgentConfig().getTimeoutRequested().ifPresent(v3Builder::withTimeout);

        return v3Builder.build();
    }

    /**
     * Convert the V4 values supplied to how the tags would have looked in Genie V3.
     *
     * @param id   The id of the resource
     * @param name The name of the resource
     * @param tags The tags on the resource
     * @return The set of tags as they would have been in Genie 3
     */
    public static ImmutableSet<String> toV3Tags(final String id, final String name, final Set<String> tags) {
        final ImmutableSet.Builder<String> v3Tags = ImmutableSet.builder();
        v3Tags.addAll(tags);
        v3Tags.add(GENIE_ID_PREFIX + id);
        v3Tags.add(GENIE_NAME_PREFIX + name);
        return v3Tags.build();
    }

    /**
     * Convert a given V4 {@code criterion} to the equivalent representation in V3 set of tags.
     *
     * @param criterion The {@link Criterion} to convert
     * @return A set of String's representing the criterion tags as they would have looked in V3
     */
    public static ImmutableSet<String> toV3CriterionTags(final Criterion criterion) {
        final ImmutableSet.Builder<String> tags = ImmutableSet.builder();
        criterion.getId().ifPresent(id -> tags.add(GENIE_ID_PREFIX + id));
        criterion.getName().ifPresent(name -> tags.add(GENIE_NAME_PREFIX + name));
        tags.addAll(criterion.getTags());
        return tags.build();
    }

    /**
     * Convert the given {@code criterion} to a V3 {@link ClusterCriteria} object.
     *
     * @param criterion The {@link Criterion} to convert
     * @return The V3 criteria object
     */
    public static ClusterCriteria toClusterCriteria(final Criterion criterion) {
        return new ClusterCriteria(toV3CriterionTags(criterion));
    }

    /**
     * Convert a V3 Cluster Criteria to a V4 Criterion.
     *
     * @param criteria The criteria to convert
     * @return The criterion
     * @throws GeniePreconditionException If the criteria converts to an invalid criterion
     */
    public static Criterion toV4Criterion(final ClusterCriteria criteria) throws GeniePreconditionException {
        return toV4Criterion(criteria.getTags());
    }

    /**
     * Convert a set of V3 criterion tags to a V4 criterion object.
     *
     * @param tags The tags to convert
     * @return The V4 criterion
     * @throws GeniePreconditionException If the tags convert to an invalid criterion
     */
    public static Criterion toV4Criterion(final Set<String> tags) throws GeniePreconditionException {
        final Criterion.Builder builder = new Criterion.Builder();
        final Set<String> v4Tags = Sets.newHashSet();
        for (final String tag : tags) {
            if (tag.startsWith(GENIE_ID_PREFIX)) {
                builder.withId(tag.substring(GENIE_ID_PREFIX.length()));
            } else if (tag.startsWith(GENIE_NAME_PREFIX)) {
                builder.withName(tag.substring(GENIE_NAME_PREFIX.length()));
            } else {
                v4Tags.add(tag);
            }
        }
        builder.withTags(v4Tags);

        // Maintain previous contract for this method even though Criterion now throws a IAE
        try {
            return builder.build();
        } catch (final IllegalArgumentException e) {
            throw new GeniePreconditionException(e.getMessage(), e);
        }
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.ApplicationStatus} to a V4 {@link ApplicationStatus}.
     *
     * @param v3Status The V3 status to convert
     * @return The V4 status the V3 status maps to
     * @throws IllegalArgumentException if the V3 status has no current V4 mapping
     */
    public static ApplicationStatus toV4ApplicationStatus(
        final com.netflix.genie.common.dto.ApplicationStatus v3Status
    ) throws IllegalArgumentException {
        switch (v3Status) {
            case ACTIVE:
                return ApplicationStatus.ACTIVE;
            case INACTIVE:
                return ApplicationStatus.INACTIVE;
            case DEPRECATED:
                return ApplicationStatus.DEPRECATED;
            default:
                throw new IllegalArgumentException("Unmapped V3 status: " + v3Status);
        }
    }

    /**
     * Attempt to convert an Application status string into a known enumeration value from {@link ApplicationStatus}.
     *
     * @param status The status string. Not null or empty.
     * @return An {@link ApplicationStatus} instance
     * @throws IllegalArgumentException If the string couldn't be converted
     */
    public static ApplicationStatus toV4ApplicationStatus(final String status) throws IllegalArgumentException {
        if (StringUtils.isBlank(status)) {
            throw new IllegalArgumentException("No application status entered. Unable to convert.");
        }
        final String upperCaseStatus = status.toUpperCase();
        try {
            return ApplicationStatus.valueOf(upperCaseStatus);
        } catch (final IllegalArgumentException e) {
            // TODO: Remove this eventually once we're satisfied v3 statuses have been flushed
            // Since it may be a remnant of V3 try the older one and map
            return toV4ApplicationStatus(com.netflix.genie.common.dto.ApplicationStatus.valueOf(upperCaseStatus));
        }
    }

    /**
     * Convert a V4 {@link ApplicationStatus} to a V3 {@link com.netflix.genie.common.dto.ApplicationStatus}.
     *
     * @param v4Status The V4 status to convert
     * @return The V3 status the V4 status maps to
     * @throws IllegalArgumentException If the V4 status has no current V3 mapping
     */
    public static com.netflix.genie.common.dto.ApplicationStatus toV3ApplicationStatus(
        final ApplicationStatus v4Status
    ) throws IllegalArgumentException {
        switch (v4Status) {
            case ACTIVE:
                return com.netflix.genie.common.dto.ApplicationStatus.ACTIVE;
            case INACTIVE:
                return com.netflix.genie.common.dto.ApplicationStatus.INACTIVE;
            case DEPRECATED:
                return com.netflix.genie.common.dto.ApplicationStatus.DEPRECATED;
            default:
                throw new IllegalArgumentException("Unmapped V4 status: " + v4Status);
        }
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.CommandStatus} to a V4 {@link CommandStatus}.
     *
     * @param v3Status The V3 status to convert
     * @return The V4 status the V3 status maps to
     * @throws IllegalArgumentException if the V3 status has no current V4 mapping
     */
    public static CommandStatus toV4CommandStatus(
        final com.netflix.genie.common.dto.CommandStatus v3Status
    ) throws IllegalArgumentException {
        switch (v3Status) {
            case ACTIVE:
                return CommandStatus.ACTIVE;
            case INACTIVE:
                return CommandStatus.INACTIVE;
            case DEPRECATED:
                return CommandStatus.DEPRECATED;
            default:
                throw new IllegalArgumentException("Unmapped V3 status: " + v3Status);
        }
    }

    /**
     * Attempt to convert a Command status string into a known enumeration value from {@link CommandStatus}.
     *
     * @param status The status string. Not null or empty.
     * @return An {@link CommandStatus} instance
     * @throws IllegalArgumentException If the string couldn't be converted
     */
    public static CommandStatus toV4CommandStatus(final String status) throws IllegalArgumentException {
        if (StringUtils.isBlank(status)) {
            throw new IllegalArgumentException("No command status entered. Unable to convert.");
        }
        final String upperCaseStatus = status.toUpperCase();
        try {
            return CommandStatus.valueOf(upperCaseStatus);
        } catch (final IllegalArgumentException e) {
            // TODO: Remove this eventually once we're satisfied v3 statuses have been flushed
            // Since it may be a remnant of V3 try the older one and map
            return toV4CommandStatus(com.netflix.genie.common.dto.CommandStatus.valueOf(upperCaseStatus));
        }
    }

    /**
     * Convert a V4 {@link CommandStatus} to a V3 {@link com.netflix.genie.common.dto.CommandStatus}.
     *
     * @param v4Status The V4 status to convert
     * @return The V3 status the V4 status maps to
     * @throws IllegalArgumentException If the V4 status has no current V3 mapping
     */
    public static com.netflix.genie.common.dto.CommandStatus toV3CommandStatus(
        final CommandStatus v4Status
    ) throws IllegalArgumentException {
        switch (v4Status) {
            case ACTIVE:
                return com.netflix.genie.common.dto.CommandStatus.ACTIVE;
            case INACTIVE:
                return com.netflix.genie.common.dto.CommandStatus.INACTIVE;
            case DEPRECATED:
                return com.netflix.genie.common.dto.CommandStatus.DEPRECATED;
            default:
                throw new IllegalArgumentException("Unmapped V4 status: " + v4Status);
        }
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.ClusterStatus} to a V4 {@link ClusterStatus}.
     *
     * @param v3Status The V3 status to convert
     * @return The V4 status the V3 status maps to
     * @throws IllegalArgumentException if the V3 status has no current V4 mapping
     */
    public static ClusterStatus toV4ClusterStatus(
        final com.netflix.genie.common.dto.ClusterStatus v3Status
    ) throws IllegalArgumentException {
        switch (v3Status) {
            case UP:
                return ClusterStatus.UP;
            case TERMINATED:
                return ClusterStatus.TERMINATED;
            case OUT_OF_SERVICE:
                return ClusterStatus.OUT_OF_SERVICE;
            default:
                throw new IllegalArgumentException("Unmapped V3 status: " + v3Status);
        }
    }

    /**
     * Attempt to convert a Cluster status string into a known enumeration value from {@link ClusterStatus}.
     *
     * @param status The status string. Not null or empty.
     * @return An {@link ClusterStatus} instance
     * @throws IllegalArgumentException If the string couldn't be converted
     */
    public static ClusterStatus toV4ClusterStatus(final String status) throws IllegalArgumentException {
        if (StringUtils.isBlank(status)) {
            throw new IllegalArgumentException("No cluster status entered. Unable to convert.");
        }
        final String upperCaseStatus = status.toUpperCase();
        try {
            return ClusterStatus.valueOf(upperCaseStatus);
        } catch (final IllegalArgumentException e) {
            // TODO: Remove this eventually once we're satisfied v3 statuses have been flushed
            // Since it may be a remnant of V3 try the older one and map
            return toV4ClusterStatus(com.netflix.genie.common.dto.ClusterStatus.valueOf(upperCaseStatus));
        }
    }

    /**
     * Convert a V4 {@link ClusterStatus} to a V3 {@link com.netflix.genie.common.dto.ClusterStatus}.
     *
     * @param v4Status The V4 status to convert
     * @return The V3 status the V4 status maps to
     * @throws IllegalArgumentException If the V4 status has no current V3 mapping
     */
    public static com.netflix.genie.common.dto.ClusterStatus toV3ClusterStatus(
        final ClusterStatus v4Status
    ) throws IllegalArgumentException {
        switch (v4Status) {
            case UP:
                return com.netflix.genie.common.dto.ClusterStatus.UP;
            case TERMINATED:
                return com.netflix.genie.common.dto.ClusterStatus.TERMINATED;
            case OUT_OF_SERVICE:
                return com.netflix.genie.common.dto.ClusterStatus.OUT_OF_SERVICE;
            default:
                throw new IllegalArgumentException("Unmapped V4 status: " + v4Status);
        }
    }

    /**
     * Convert a V3 {@link com.netflix.genie.common.dto.JobStatus} to a V4 {@link JobStatus}.
     *
     * @param v3Status The V3 status to convert
     * @return The V4 status the V3 status maps to
     * @throws IllegalArgumentException if the V3 status has no current V4 mapping
     */
    public static JobStatus toV4JobStatus(
        final com.netflix.genie.common.dto.JobStatus v3Status
    ) throws IllegalArgumentException {
        switch (v3Status) {
            case ACCEPTED:
                return JobStatus.ACCEPTED;
            case CLAIMED:
                return JobStatus.CLAIMED;
            case FAILED:
                return JobStatus.FAILED;
            case INIT:
                return JobStatus.INIT;
            case INVALID:
                return JobStatus.INVALID;
            case KILLED:
                return JobStatus.KILLED;
            case RESERVED:
                return JobStatus.RESERVED;
            case RESOLVED:
                return JobStatus.RESOLVED;
            case RUNNING:
                return JobStatus.RUNNING;
            case SUCCEEDED:
                return JobStatus.SUCCEEDED;
            default:
                throw new IllegalArgumentException("Unmapped V3 status: " + v3Status);
        }
    }

    /**
     * Convert a V4 {@link JobStatus} to a V3 {@link com.netflix.genie.common.dto.JobStatus}.
     *
     * @param v4Status The V4 status to convert
     * @return The V3 status the V4 status maps to
     * @throws IllegalArgumentException If the V4 status has no current V3 mapping
     */
    public static com.netflix.genie.common.dto.JobStatus toV3JobStatus(
        final JobStatus v4Status
    ) throws IllegalArgumentException {
        switch (v4Status) {
            case ACCEPTED:
                return com.netflix.genie.common.dto.JobStatus.ACCEPTED;
            case CLAIMED:
                return com.netflix.genie.common.dto.JobStatus.CLAIMED;
            case FAILED:
                return com.netflix.genie.common.dto.JobStatus.FAILED;
            case INIT:
                return com.netflix.genie.common.dto.JobStatus.INIT;
            case INVALID:
                return com.netflix.genie.common.dto.JobStatus.INVALID;
            case KILLED:
                return com.netflix.genie.common.dto.JobStatus.KILLED;
            case RESERVED:
                return com.netflix.genie.common.dto.JobStatus.RESERVED;
            case RESOLVED:
                return com.netflix.genie.common.dto.JobStatus.RESOLVED;
            case RUNNING:
                return com.netflix.genie.common.dto.JobStatus.RUNNING;
            case SUCCEEDED:
                return com.netflix.genie.common.dto.JobStatus.SUCCEEDED;
            default:
                throw new IllegalArgumentException("Unmapped V4 status: " + v4Status);
        }
    }

    /**
     * Attempt to convert a Job status string into a known enumeration value from {@link JobStatus}.
     *
     * @param status The status string. Not null or empty.
     * @return A {@link JobStatus} instance
     * @throws IllegalArgumentException If the string couldn't be converted
     */
    public static JobStatus toV4JobStatus(final String status) throws IllegalArgumentException {
        if (StringUtils.isBlank(status)) {
            throw new IllegalArgumentException("No job status entered. Unable to convert.");
        }
        final String upperCaseStatus = status.toUpperCase();
        try {
            return JobStatus.valueOf(upperCaseStatus);
        } catch (final IllegalArgumentException e) {
            // TODO: Remove this eventually once we're satisfied v3 statuses have been flushed
            // Since it may be a remnant of V3 try the older one and map
            return toV4JobStatus(com.netflix.genie.common.dto.JobStatus.valueOf(upperCaseStatus));
        }
    }

    private static Set<String> toV4Tags(final Set<String> tags) {
        return tags
            .stream()
            .filter(tag -> !tag.startsWith(GENIE_ID_PREFIX) && !tag.startsWith(GENIE_NAME_PREFIX))
            .collect(Collectors.toSet());
    }
}
