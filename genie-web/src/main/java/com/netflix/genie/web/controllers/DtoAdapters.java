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
package com.netflix.genie.web.controllers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.v4.Application;
import com.netflix.genie.common.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.dto.v4.ApplicationRequest;
import com.netflix.genie.common.dto.v4.Cluster;
import com.netflix.genie.common.dto.v4.ClusterMetadata;
import com.netflix.genie.common.dto.v4.ClusterRequest;
import com.netflix.genie.common.dto.v4.Command;
import com.netflix.genie.common.dto.v4.CommandMetadata;
import com.netflix.genie.common.dto.v4.CommandRequest;
import com.netflix.genie.common.dto.v4.Criterion;
import com.netflix.genie.common.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.dto.v4.JobMetadata;
import com.netflix.genie.common.dto.v4.JobRequest;
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
public final class DtoAdapters {

    static final String NO_VERSION_SPECIFIED = "No Version Specified";
    static final String GENIE_ID_PREFIX = "genie.id:";
    static final String GENIE_NAME_PREFIX = "genie.name:";

    private DtoAdapters() {
    }

    static ApplicationRequest toV4ApplicationRequest(
        final com.netflix.genie.common.dto.Application v3Application
    ) {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            v3Application.getName(),
            v3Application.getUser(),
            v3Application.getStatus()
        )
            .withVersion(v3Application.getVersion())
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
     */
    public static Application toV4Application(
        final com.netflix.genie.common.dto.Application v3Application
    ) {
        final ApplicationMetadata.Builder metadataBuilder = new ApplicationMetadata.Builder(
            v3Application.getName(),
            v3Application.getUser(),
            v3Application.getStatus()
        )
            .withVersion(v3Application.getVersion())
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
     */
    public static com.netflix.genie.common.dto.Application toV3Application(final Application v4Application) {
        final ApplicationMetadata applicationMetadata = v4Application.getMetadata();
        final ExecutionEnvironment resources = v4Application.getResources();
        final com.netflix.genie.common.dto.Application.Builder builder
            = new com.netflix.genie.common.dto.Application.Builder(
            applicationMetadata.getName(),
            applicationMetadata.getUser(),
            applicationMetadata.getVersion().orElse(NO_VERSION_SPECIFIED),
            applicationMetadata.getStatus()
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

    static ClusterRequest toV4ClusterRequest(final com.netflix.genie.common.dto.Cluster v3Cluster) {
        final ClusterMetadata.Builder metadataBuilder = new ClusterMetadata.Builder(
            v3Cluster.getName(),
            v3Cluster.getUser(),
            v3Cluster.getStatus()
        )
            .withVersion(v3Cluster.getVersion())
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

    static com.netflix.genie.common.dto.Cluster toV3Cluster(final Cluster v4Cluster) {
        final ClusterMetadata clusterMetadata = v4Cluster.getMetadata();
        final ExecutionEnvironment resources = v4Cluster.getResources();
        final com.netflix.genie.common.dto.Cluster.Builder builder
            = new com.netflix.genie.common.dto.Cluster.Builder(
            clusterMetadata.getName(),
            clusterMetadata.getUser(),
            clusterMetadata.getVersion().orElse(NO_VERSION_SPECIFIED),
            clusterMetadata.getStatus()
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

    static CommandRequest toV4CommandRequest(final com.netflix.genie.common.dto.Command v3Command) {
        final CommandMetadata.Builder metadataBuilder = new CommandMetadata.Builder(
            v3Command.getName(),
            v3Command.getUser(),
            v3Command.getStatus()
        )
            .withVersion(v3Command.getVersion())
            .withTags(toV4Tags(v3Command.getTags()));

        v3Command.getMetadata().ifPresent(metadataBuilder::withMetadata);
        v3Command.getDescription().ifPresent(metadataBuilder::withDescription);

        final List<String> executable = Lists.newArrayList(StringUtils.split(v3Command.getExecutable(), ' '));
        final CommandRequest.Builder builder = new CommandRequest.Builder(metadataBuilder.build(), executable);
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

    static com.netflix.genie.common.dto.Command toV3Command(final Command v4Command) {
        final CommandMetadata commandMetadata = v4Command.getMetadata();
        final ExecutionEnvironment resources = v4Command.getResources();
        final com.netflix.genie.common.dto.Command.Builder builder
            = new com.netflix.genie.common.dto.Command.Builder(
            commandMetadata.getName(),
            commandMetadata.getUser(),
            commandMetadata.getVersion().orElse(NO_VERSION_SPECIFIED),
            commandMetadata.getStatus(),
            StringUtils.join(v4Command.getExecutable(), ' '),
            com.netflix.genie.common.dto.Command.DEFAULT_CHECK_DELAY
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

//    public static JobRequest toV4JobRequest(com.netflix.genie.common.dto.JobRequest v3JobRequest) {
//
//    }

    /**
     * Helper method to convert a v4 JobRequest to a v3 job request.
     *
     * @param v4JobRequest The v4 job request instance
     * @return The v3 job request instance
     */
    static com.netflix.genie.common.dto.JobRequest toV3JobRequest(final JobRequest v4JobRequest) {
        final com.netflix.genie.common.dto.JobRequest.Builder v3Builder
            = new com.netflix.genie.common.dto.JobRequest.Builder(
            v4JobRequest.getMetadata().getName(),
            v4JobRequest.getMetadata().getUser(),
            v4JobRequest.getMetadata().getVersion().orElse(NO_VERSION_SPECIFIED),
            v4JobRequest
                .getCriteria()
                .getClusterCriteria()
                .stream()
                .map(DtoAdapters::toClusterCriteria)
                .collect(Collectors.toList()),
            toV3CriterionTags(v4JobRequest.getCriteria().getCommandCriterion())
        )
            .withApplications(v4JobRequest.getCriteria().getApplicationIds())
            .withCommandArgs(v4JobRequest.getCommandArgs())
            .withDisableLogArchival(v4JobRequest.isArchivingDisabled())
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

        v4JobRequest.getTimeout().ifPresent(v3Builder::withTimeout);

        return v3Builder.build();
    }

    private static ImmutableSet<String> toV3Tags(final String id, final String name, final Set<String> tags) {
        final ImmutableSet.Builder<String> v3Tags = ImmutableSet.builder();
        v3Tags.addAll(tags);
        v3Tags.add(GENIE_ID_PREFIX + id);
        v3Tags.add(GENIE_NAME_PREFIX + name);
        return v3Tags.build();
    }

    private static Set<String> toV4Tags(final Set<String> tags) {
        return tags
            .stream()
            .filter(tag -> !tag.startsWith(GENIE_ID_PREFIX) && !tag.startsWith(GENIE_NAME_PREFIX))
            .collect(Collectors.toSet());
    }

    static ImmutableSet<String> toV3CriterionTags(final Criterion criterion) {
        final ImmutableSet.Builder<String> tags = ImmutableSet.builder();
        criterion.getId().ifPresent(id -> tags.add(GENIE_ID_PREFIX + id));
        criterion.getName().ifPresent(name -> tags.add(GENIE_NAME_PREFIX + name));
        tags.addAll(criterion.getTags());
        return tags.build();
    }

    private static ClusterCriteria toClusterCriteria(final Criterion criterion) {
        return new ClusterCriteria(toV3CriterionTags(criterion));
    }
}
