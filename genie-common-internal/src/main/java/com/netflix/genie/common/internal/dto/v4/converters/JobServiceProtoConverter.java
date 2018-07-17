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
package com.netflix.genie.common.internal.dto.v4.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.proto.AgentMetadata;
import com.netflix.genie.proto.ChangeJobStatusRequest;
import com.netflix.genie.proto.ClaimJobRequest;
import com.netflix.genie.proto.DryRunJobSpecificationRequest;
import com.netflix.genie.proto.ExecutionResource;
import com.netflix.genie.proto.JobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converter of proto messages for the {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceImplBase} service to
 * and from V4 DTO POJO's.
 *
 * @author tgianos
 * @see com.netflix.genie.common.internal.dto.v4
 * @see com.netflix.genie.proto
 * @since 4.0.0
 */
@Component
public class JobServiceProtoConverter {

    /**
     * Convert a V4 Job Request DTO into a gRPC reserve job id request to be sent to the server.
     *
     * @param jobRequest          The job request to convert
     * @param agentClientMetadata The metadata about the agent
     * @return The request that should be sent to the server for a new Job Specification given the parameters
     * @throws GenieConversionException if conversion fails
     */
    public ReserveJobIdRequest toProtoReserveJobIdRequest(
        final AgentJobRequest jobRequest,
        final AgentClientMetadata agentClientMetadata
    ) throws GenieConversionException {
        final ReserveJobIdRequest.Builder builder = ReserveJobIdRequest.newBuilder();
        builder.setMetadata(toProtoJobMetadata(jobRequest));
        builder.setCriteria(toProtoExecutionResourceCriteria(jobRequest));
        builder.setAgentConfig(toProtoAgentConfig(jobRequest));
        builder.setAgentMetadata(toProtoAgentMetadata(agentClientMetadata));
        return builder.build();
    }

    /**
     * Generate a {@link JobSpecificationRequest} from the given job id.
     *
     * @param id The job id to generate the request for
     * @return The request instance
     */
    public JobSpecificationRequest toProtoJobSpecificationRequest(final String id) {
        return JobSpecificationRequest.newBuilder().setId(id).build();
    }

    /**
     * Convert a gRPC reserve job id request into a V4 Job Request DTO for use within Genie codebase.
     *
     * @param request The request to convert
     * @return The job request
     * @throws GenieConversionException if conversion fails
     */
    public JobRequest toJobRequestDTO(final ReserveJobIdRequest request) throws GenieConversionException {
        return toJobRequest(request.getMetadata(), request.getCriteria(), request.getAgentConfig());
    }

    /**
     * Convert a V4 Job Request DTO into a gRPC dry run resolve job specification request to be sent to the server.
     *
     * @param jobRequest The job request to convert
     * @return The request that should be sent to the server for a new Job Specification given the parameters
     * @throws GenieConversionException if conversion fails
     */
    public DryRunJobSpecificationRequest toProtoDryRunJobSpecificationRequest(
        final AgentJobRequest jobRequest
    ) throws GenieConversionException {
        final DryRunJobSpecificationRequest.Builder builder = DryRunJobSpecificationRequest.newBuilder();
        builder.setMetadata(toProtoJobMetadata(jobRequest));
        builder.setCriteria(toProtoExecutionResourceCriteria(jobRequest));
        builder.setAgentConfig(toProtoAgentConfig(jobRequest));
        return builder.build();
    }

    /**
     * Convert a gRPC request to dry run a job specification resolution into a {@link JobRequest} for use within
     * Genie server codebase.
     *
     * @param request The request to convert
     * @return The job request
     * @throws GenieConversionException if conversion fails
     */
    public JobRequest toJobRequestDTO(
        final DryRunJobSpecificationRequest request
    ) throws GenieConversionException {
        return toJobRequest(request.getMetadata(), request.getCriteria(), request.getAgentConfig());
    }

    /**
     * Convert a proto {@link AgentMetadata} to an {@link AgentClientMetadata}.
     *
     * @param agentMetadata The metadata to convert
     * @return The immutable DTO representation
     */
    public AgentClientMetadata toAgentClientMetadataDTO(final AgentMetadata agentMetadata) {
        return new AgentClientMetadata(
            agentMetadata.getAgentHostname(),
            agentMetadata.getAgentVersion(),
            agentMetadata.getAgentPid()
        );
    }

    /**
     * Build a {@link JobSpecificationResponse} out of the given {@link JobSpecification}.
     *
     * @param jobSpecification The job specification to serialize
     * @return The response instance
     */
    public JobSpecificationResponse toProtoJobSpecificationResponse(final JobSpecification jobSpecification) {
        return JobSpecificationResponse
            .newBuilder()
            .setSpecification(toProtoJobSpecification(jobSpecification))
            .build();
    }

    /**
     * Convert a response from server into a Job Specification DTO which can be used in the codebase free of gRPC.
     *
     * @param protoSpec The protobuf specification message
     * @return A job specification DTO
     */
    public JobSpecification toJobSpecificationDTO(
        final com.netflix.genie.proto.JobSpecification protoSpec
    ) {
        return new JobSpecification(
            protoSpec.getCommandArgsList(),
            toExecutionResourceDTO(protoSpec.getJob()),
            toExecutionResourceDTO(protoSpec.getCluster()),
            toExecutionResourceDTO(protoSpec.getCommand()),
            protoSpec
                .getApplicationsList()
                .stream()
                .map(JobServiceProtoConverter::toExecutionResourceDTO)
                .collect(Collectors.toList()),
            protoSpec.getEnvironmentVariablesMap(),
            protoSpec.getIsInteractive(),
            new File(protoSpec.getJobDirectoryLocation())
        );
    }

    /**
     * Convert agent metadata and job id into a ClaimJobRequest for the server.
     *
     * @param jobId               job id
     * @param agentClientMetadata agent metadata
     * @return a ClaimJobRequest
     */
    public ClaimJobRequest toProtoClaimJobRequest(
        final String jobId,
        final AgentClientMetadata agentClientMetadata
    ) {
        return ClaimJobRequest.newBuilder()
            .setId(jobId)
            .setAgentMetadata(
                toProtoAgentMetadata(agentClientMetadata)
            )
            .build();
    }

    /**
     * Convert parameters into ChangeJobStatusRequest for the server.
     *
     * @param jobId            job id
     * @param currentJobStatus the expected current status on the server
     * @param newJobStatus     the new current status for this job
     * @param message          an optional message to record with the state change
     * @return a ChangeJobStatusRequest
     */
    public ChangeJobStatusRequest toProtoChangeJobStatusRequest(
        final @NotBlank String jobId,
        final JobStatus currentJobStatus,
        final JobStatus newJobStatus,
        final @Nullable String message
    ) {
        return ChangeJobStatusRequest.newBuilder()
            .setId(jobId)
            .setCurrentStatus(currentJobStatus.name())
            .setNewStatus(newJobStatus.name())
            .setNewStatusMessage(message == null ? "" : message)
            .build();
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
                .map(JobServiceProtoConverter::toProtoExecutionResource)
                .collect(Collectors.toList())
        );
        builder.putAllEnvironmentVariables(jobSpecification.getEnvironmentVariables());
        builder.setIsInteractive(jobSpecification.isInteractive());
        builder.setJobDirectoryLocation(jobSpecification.getJobDirectoryLocation().getAbsolutePath());
        return builder.build();
    }

    private static JobSpecification.ExecutionResource toExecutionResourceDTO(final ExecutionResource protoResource) {
        return new JobSpecification.ExecutionResource(
            protoResource.getId(),
            new ExecutionEnvironment(
                ImmutableSet.copyOf(protoResource.getConfigsList()),
                ImmutableSet.copyOf(protoResource.getDependenciesList()),
                protoResource.getSetupFile().isEmpty() ? null : protoResource.getSetupFile()
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
            .addAllDependencies(executionResource.getExecutionEnvironment().getDependencies());

        executionResource.getExecutionEnvironment().getSetupFile().ifPresent(builder::setSetupFile);

        return builder.build();
    }

    private static Criterion toCriterionDTO(
        final com.netflix.genie.proto.Criterion protoCriterion
    ) throws GenieConversionException {
        try {
            return new Criterion
                .Builder()
                .withId(protoCriterion.getId())
                .withName(protoCriterion.getName())
                .withVersion(protoCriterion.getVersion())
                .withStatus(protoCriterion.getStatus())
                .withTags(protoCriterion.getTagsList() != null ? Sets.newHashSet(protoCriterion.getTagsList()) : null)
                .build();
        } catch (final GeniePreconditionException e) {
            throw new GenieConversionException("Failed to convert criterion", e);
        }
    }

    private static com.netflix.genie.proto.Criterion toProtoCriterion(final Criterion criterion) {
        final com.netflix.genie.proto.Criterion.Builder builder = com.netflix.genie.proto.Criterion.newBuilder();
        criterion.getId().ifPresent(builder::setId);
        criterion.getName().ifPresent(builder::setName);
        criterion.getVersion().ifPresent(builder::setVersion);
        criterion.getStatus().ifPresent(builder::setStatus);
        builder.addAllTags(criterion.getTags());
        return builder.build();
    }

    private static com.netflix.genie.proto.JobMetadata toProtoJobMetadata(
        final AgentJobRequest jobRequest
    ) throws GenieConversionException {
        final JobMetadata jobMetadata = jobRequest.getMetadata();
        final ExecutionEnvironment jobResources = jobRequest.getResources();

        final com.netflix.genie.proto.JobMetadata.Builder builder
            = com.netflix.genie.proto.JobMetadata.newBuilder();
        jobRequest.getRequestedId().ifPresent(builder::setId);
        builder.setName(jobMetadata.getName());
        builder.setUser(jobMetadata.getUser());
        builder.setVersion(jobMetadata.getVersion());
        jobMetadata.getDescription().ifPresent(builder::setDescription);
        builder.addAllTags(jobMetadata.getTags());
        if (jobMetadata.getMetadata().isPresent()) {
            final String serializedMetadata;
            try {
                serializedMetadata = GenieObjectMapper.getMapper().writeValueAsString(jobMetadata.getMetadata().get());
            } catch (JsonProcessingException e) {
                throw new GenieConversionException("Failed to serialize job metadata to JSON", e);
            }
            builder.setMetadata(serializedMetadata);
        }
        jobMetadata.getEmail().ifPresent(builder::setEmail);
        jobMetadata.getGrouping().ifPresent(builder::setGrouping);
        jobMetadata.getGroupingInstance().ifPresent(builder::setGroupingInstance);
        jobResources.getSetupFile().ifPresent(builder::setSetupFile);
        builder.addAllConfigs(jobResources.getConfigs());
        builder.addAllDependencies(jobResources.getDependencies());
        builder.addAllCommandArgs(jobRequest.getCommandArgs());
        return builder.build();
    }

    private static com.netflix.genie.proto.ExecutionResourceCriteria toProtoExecutionResourceCriteria(
        final AgentJobRequest jobRequest
    ) {
        final ExecutionResourceCriteria executionResourceCriteria = jobRequest.getCriteria();
        final com.netflix.genie.proto.ExecutionResourceCriteria.Builder builder
            = com.netflix.genie.proto.ExecutionResourceCriteria.newBuilder();
        builder.addAllClusterCriteria(
            executionResourceCriteria
                .getClusterCriteria()
                .stream()
                .map(JobServiceProtoConverter::toProtoCriterion)
                .collect(Collectors.toList())
        );
        builder.setCommandCriterion(
            toProtoCriterion(executionResourceCriteria.getCommandCriterion())
        );
        builder.addAllRequestedApplicationIdOverrides(executionResourceCriteria.getApplicationIds());
        return builder.build();
    }

    private static com.netflix.genie.proto.AgentConfig toProtoAgentConfig(final AgentJobRequest jobRequest) {
        final com.netflix.genie.proto.AgentConfig.Builder builder = com.netflix.genie.proto.AgentConfig.newBuilder();
        final AgentConfigRequest agentConfigRequest = jobRequest.getRequestedAgentConfig();
        agentConfigRequest.getRequestedJobDirectoryLocation().ifPresent(
            location -> builder.setJobDirectoryLocation(location.getAbsolutePath())
        );
        builder.setIsInteractive(agentConfigRequest.isInteractive());
        return builder.build();
    }

    private static AgentMetadata toProtoAgentMetadata(final AgentClientMetadata agentClientMetadata) {
        final AgentMetadata.Builder builder = AgentMetadata.newBuilder();
        agentClientMetadata.getHostname().ifPresent(builder::setAgentHostname);
        agentClientMetadata.getVersion().ifPresent(builder::setAgentVersion);
        agentClientMetadata.getPid().ifPresent(builder::setAgentPid);
        return builder.build();
    }

    private static JobRequest toJobRequest(
        final com.netflix.genie.proto.JobMetadata protoJobMetadata,
        final com.netflix.genie.proto.ExecutionResourceCriteria protoExecutionResourceCriteria,
        final com.netflix.genie.proto.AgentConfig protoAgentConfig
    ) throws GenieConversionException {
        try {
            final JobMetadata jobMetadata = new JobMetadata.Builder(
                protoJobMetadata.getName(),
                protoJobMetadata.getUser(),
                protoJobMetadata.getVersion()
            )
                .withDescription(protoJobMetadata.getDescription())
                .withTags(
                    protoJobMetadata.getTagsList() != null
                        ? Sets.newHashSet(protoJobMetadata.getTagsList())
                        : null
                )
                .withMetadata(protoJobMetadata.getMetadata())
                .withEmail(protoJobMetadata.getEmail())
                .withGrouping(protoJobMetadata.getGrouping())
                .withGroupingInstance(protoJobMetadata.getGroupingInstance())
                .build();

            final List<com.netflix.genie.proto.Criterion> protoCriteria
                = protoExecutionResourceCriteria.getClusterCriteriaList();
            final List<Criterion> clusterCriteria = Lists.newArrayListWithExpectedSize(protoCriteria.size());
            for (final com.netflix.genie.proto.Criterion protoCriterion : protoCriteria) {
                clusterCriteria.add(toCriterionDTO(protoCriterion));
            }

            final ExecutionResourceCriteria executionResourceCriteria = new ExecutionResourceCriteria(
                clusterCriteria,
                toCriterionDTO(protoExecutionResourceCriteria.getCommandCriterion()),
                protoExecutionResourceCriteria.getRequestedApplicationIdOverridesList()
            );

            final ExecutionEnvironment jobResources = new ExecutionEnvironment(
                protoJobMetadata.getConfigsList() != null
                    ? Sets.newHashSet(protoJobMetadata.getConfigsList())
                    : null,
                protoJobMetadata.getDependenciesList() != null
                    ? Sets.newHashSet(protoJobMetadata.getDependenciesList())
                    : null,
                protoJobMetadata.getSetupFile()
            );

            final AgentConfigRequest agentConfigRequest = new AgentConfigRequest
                .Builder()
                .withRequestedJobDirectoryLocation(protoAgentConfig.getJobDirectoryLocation())
                .withInteractive(protoAgentConfig.getIsInteractive())
                .build();

            final String jobId = protoJobMetadata.getId();

            return new JobRequest(
                StringUtils.isBlank(jobId) ? null : jobId,
                jobResources,
                protoJobMetadata.getCommandArgsList(),
                jobMetadata,
                executionResourceCriteria,
                null,
                agentConfigRequest
            );
        } catch (GeniePreconditionException e) {
            throw new GenieConversionException("Failed to compose JobRequest", e);
        }
    }
}
