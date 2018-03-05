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
package com.netflix.genie.proto.v4.adapters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.v4.Criterion;
import com.netflix.genie.common.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.dto.v4.JobMetadata;
import com.netflix.genie.common.dto.v4.JobRequest;
import com.netflix.genie.common.dto.v4.JobSpecification;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.proto.ExecutionResource;
import com.netflix.genie.proto.GetJobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.JobSpecificationServiceError;
import com.netflix.genie.proto.ResolveJobSpecificationRequest;
import io.opencensus.internal.StringUtil;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for converting proto messages for the
 * {@link com.netflix.genie.proto.JobSpecificationServiceGrpc.JobSpecificationServiceImplBase} service to and from
 * V4 DTO POJO's.
 *
 * @author tgianos
 * @see com.netflix.genie.common.dto.v4
 * @see com.netflix.genie.proto
 * @since 4.0.0
 */
public final class JobSpecificationServiceAdapter {

    /**
     * Utility class shouldn't be constructed.
     */
    private JobSpecificationServiceAdapter() {
    }

    /**
     * Convert a V4 Job Request DTO into a gRPC resolve job specification request to be sent to the server.
     *
     * @param jobRequest The job request to convert
     * @return The request that should be sent to the server for a new Job Specification given the parameters
     */
    // TODO: Make it throw a runtime exception
    public static ResolveJobSpecificationRequest toProtoResolveJobSpecificationRequest(
        final JobRequest jobRequest
    ) throws JsonProcessingException {
        return ResolveJobSpecificationRequest
            .newBuilder()
            .setMetadata(toProtoJobMetadata(jobRequest))
            .setCriteria(toProtoExecutionResourceCriteria(jobRequest.getCriteria()))
            .build();
    }

    /**
     * Generate a {@link GetJobSpecificationRequest} from the given job id.
     *
     * @param id The job id to generate the request for
     * @return The request instance
     */
    public static GetJobSpecificationRequest toProtoGetJobSpecificationRequest(final String id) {
        return GetJobSpecificationRequest.newBuilder().setId(id).build();
    }

    /**
     * Convert a gRPC resolve job specification request into a V4 Job Request DTO for use within Genie codebase.
     *
     * @param request The request to convert
     * @return The job request
     * @throws GenieException if any serialization errors occur
     */
    public static JobRequest toJobRequestDTO(final ResolveJobSpecificationRequest request) throws GenieException {
        final com.netflix.genie.proto.JobMetadata jobMetadata = request.getMetadata();
        final JobMetadata userMetadata = new JobMetadata.Builder(
            jobMetadata.getName(),
            jobMetadata.getUser()
        )
            .withVersion(jobMetadata.getVersion())
            .withDescription(jobMetadata.getDescription())
            .withTags(jobMetadata.getTagsList() != null ? Sets.newHashSet(jobMetadata.getTagsList()) : null)
            .withMetadata(jobMetadata.getMetadata())
            .withEmail(jobMetadata.getEmail())
            .withGrouping(jobMetadata.getGrouping())
            .withGroupingInstance(jobMetadata.getGroupingInstance())
            .build();

        final com.netflix.genie.proto.ExecutionResourceCriteria protoResourceCriteria = request.getCriteria();
        final List<com.netflix.genie.proto.Criterion> protoCriteria = protoResourceCriteria.getClusterCriteriaList();
        final List<Criterion> clusterCriteria = Lists.newArrayListWithExpectedSize(protoCriteria.size());
        for (final com.netflix.genie.proto.Criterion protoCriterion : protoCriteria) {
            clusterCriteria.add(toCriterionDTO(protoCriterion));
        }

        final ExecutionResourceCriteria executionResourceCriteria = new ExecutionResourceCriteria(
            clusterCriteria,
            toCriterionDTO(protoResourceCriteria.getCommandCriterion()),
            protoResourceCriteria.getApplicationIdsList()
        );

        final ExecutionEnvironment jobResources = new ExecutionEnvironment(
            jobMetadata.getConfigsList() != null ? Sets.newHashSet(jobMetadata.getConfigsList()) : null,
            jobMetadata.getDependenciesList() != null ? Sets.newHashSet(jobMetadata.getDependenciesList()) : null,
            jobMetadata.getSetupFile()
        );

        return new JobRequest.Builder(
            userMetadata,
            executionResourceCriteria
        )
            .withResources(jobResources)
            .withRequestedId(jobMetadata.getId())
            .withCommandArgs(jobMetadata.getCommandArgsList())
            .withInteractive(jobMetadata.getIsInteractive())
            .withJobDirectoryLocation(
                jobMetadata.getJobDirectoryLocation() == null ? null : new File(jobMetadata.getJobDirectoryLocation())
            )
            .build();
    }

    /**
     * Build a {@link JobSpecificationResponse} out of the given {@link JobSpecification}.
     *
     * @param jobSpecification The job specification to serialize
     * @return The response instance
     */
    public static JobSpecificationResponse toProtoJobSpecificationResponse(final JobSpecification jobSpecification) {
        return JobSpecificationResponse
            .newBuilder()
            .setSpecification(toProtoJobSpecification(jobSpecification))
            .build();
    }

    /**
     * Build a {@link JobSpecificationResponse} out of the given {@link Throwable}.
     *
     * @param error The server exception that should be serialized and sent back to the client
     * @return The response instance
     */
    public static JobSpecificationResponse toProtoJobSpecificationResponse(final Throwable error) {
        // TODO: Flesh this out with other errors but for now just throw a generic error
        return JobSpecificationResponse
            .newBuilder()
            .setError(
                JobSpecificationServiceError
                    .newBuilder()
                    .setType(JobSpecificationServiceError.Type.UNKNOWN)
                    .setMessage(error.getMessage())
                    .build()
            )
            .build();
    }

    /**
     * Convert a response from server into a Job Specification DTO which can be used in the codebase free of gRPC.
     *
     * @param response The response to parse
     * @return A job specification
     */
    public static JobSpecification toJobSpecificationDTO(final JobSpecificationResponse response) {
        if (response.hasError()) {
            // TODO: Throw exception
        }

        final com.netflix.genie.proto.JobSpecification protoSpec = response.getSpecification();
        return new JobSpecification(
            protoSpec.getCommandArgsList(),
            toExecutionResourceDTO(protoSpec.getJob()),
            toExecutionResourceDTO(protoSpec.getCluster()),
            toExecutionResourceDTO(protoSpec.getCommand()),
            protoSpec
                .getApplicationsList()
                .stream()
                .map(JobSpecificationServiceAdapter::toExecutionResourceDTO)
                .collect(Collectors.toList()),
            protoSpec.getEnvironmentVariablesMap(),
            protoSpec.getIsInteractive(),
            protoSpec.getJobDirectoryLocation() == null ? null : new File(protoSpec.getJobDirectoryLocation())
        );
    }

    private static com.netflix.genie.proto.JobSpecification toProtoJobSpecification(
        final JobSpecification jobSpecification
    ) {
        final com.netflix.genie.proto.JobSpecification.Builder builder
            = com.netflix.genie.proto.JobSpecification.newBuilder();

        builder.addAllCommandArgs(jobSpecification.getCommandArgs());
        builder.setJob(toProtoExecutionResource(jobSpecification.getJob()));
        builder.setCluster(toProtoExecutionResource(jobSpecification.getCluster()));
        builder.setCommand(toProtoExecutionResource(jobSpecification.getCommand()));
        builder.addAllApplications(
            jobSpecification
                .getApplications()
                .stream()
                .map(JobSpecificationServiceAdapter::toProtoExecutionResource)
                .collect(Collectors.toList())
        );
        builder.putAllEnvironmentVariables(jobSpecification.getEnvironmentVariables());
        builder.setIsInteractive(jobSpecification.isInteractive());
        if (jobSpecification.getJobDirectoryLocation() != null) {
            builder.setJobDirectoryLocation(jobSpecification.getJobDirectoryLocation().getAbsolutePath());
        }
        return builder.build();
    }

    private static JobSpecification.ExecutionResource toExecutionResourceDTO(final ExecutionResource protoResource) {
        return new JobSpecification.ExecutionResource(
            protoResource.getId(),
            new ExecutionEnvironment(
                ImmutableSet.copyOf(protoResource.getConfigsList()),
                ImmutableSet.copyOf(protoResource.getDependencesList()),
                protoResource.getSetupFile()
            )
        );
    }

    private static ExecutionResource toProtoExecutionResource(
        final JobSpecification.ExecutionResource executionResource
    ) {
        final ExecutionResource.Builder builder = ExecutionResource
            .newBuilder()
            .setId(executionResource.getId())
            .addAllConfigs(executionResource.getExecutionEnvironment().getConfigs())
            .addAllDependences(executionResource.getExecutionEnvironment().getDependencies());

        executionResource.getExecutionEnvironment().getSetupFile().ifPresent(builder::setSetupFile);

        return builder.build();
    }

    private static Criterion toCriterionDTO(
        final com.netflix.genie.proto.Criterion protoCriterion
    ) throws GeniePreconditionException {
        return new Criterion
            .Builder()
            .withId(protoCriterion.getId())
            .withName(protoCriterion.getName())
            .withStatus(protoCriterion.getStatus())
            .withTags(protoCriterion.getTagsList() != null ? Sets.newHashSet(protoCriterion.getTagsList()) : null)
            .build();
    }

    private static com.netflix.genie.proto.Criterion toProtoCriterion(final Criterion criterion) {
        final com.netflix.genie.proto.Criterion.Builder builder = com.netflix.genie.proto.Criterion.newBuilder();
        criterion.getId().ifPresent(builder::setId);
        criterion.getName().ifPresent(builder::setName);
        criterion.getStatus().ifPresent(builder::setStatus);
        builder.addAllTags(criterion.getTags());
        return builder.build();
    }

    private static com.netflix.genie.proto.JobMetadata toProtoJobMetadata(
        final JobRequest jobRequest
    ) throws JsonProcessingException {
        final JobMetadata userMetadata = jobRequest.getMetadata();
        final ExecutionEnvironment jobResources = jobRequest.getResources();

        final com.netflix.genie.proto.JobMetadata.Builder builder
            = com.netflix.genie.proto.JobMetadata.newBuilder();
        jobRequest.getRequestedId().ifPresent(builder::setId);
        builder.setName(userMetadata.getName());
        builder.setUser(userMetadata.getUser());
        userMetadata.getVersion().ifPresent(builder::setVersion);
        userMetadata.getDescription().ifPresent(builder::setDescription);
        builder.addAllTags(userMetadata.getTags());
        if (userMetadata.getMetadata().isPresent()) {
            builder.setMetadata(
                GenieObjectMapper.getMapper().writeValueAsString(userMetadata.getMetadata().get())
            );
        }
        userMetadata.getEmail().ifPresent(builder::setEmail);
        userMetadata.getGrouping().ifPresent(builder::setGrouping);
        userMetadata.getGroupingInstance().ifPresent(builder::setGroupingInstance);
        jobResources.getSetupFile().ifPresent(builder::setSetupFile);
        builder.addAllConfigs(jobResources.getConfigs());
        builder.addAllDependencies(jobResources.getDependencies());
        builder.addAllCommandArgs(jobRequest.getCommandArgs());
        builder.setIsInteractive(jobRequest.isInteractive());
        if (jobRequest.getJobDirectoryLocation() != null) {
            builder.setJobDirectoryLocation(jobRequest.getJobDirectoryLocation().getAbsolutePath());
        }
        return builder.build();
    }

    private static com.netflix.genie.proto.ExecutionResourceCriteria toProtoExecutionResourceCriteria(
        final ExecutionResourceCriteria executionResourceCriteria
    ) {
        final com.netflix.genie.proto.ExecutionResourceCriteria.Builder builder
            = com.netflix.genie.proto.ExecutionResourceCriteria.newBuilder();
        builder.addAllClusterCriteria(
            executionResourceCriteria
                .getClusterCriteria()
                .stream()
                .map(JobSpecificationServiceAdapter::toProtoCriterion)
                .collect(Collectors.toList())
        );
        builder.setCommandCriterion(
            toProtoCriterion(executionResourceCriteria.getCommandCriterion())
        );
        builder.addAllApplicationIds(executionResourceCriteria.getApplicationIds());
        return builder.build();
    }
}
