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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Int32Value;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException;
import com.netflix.genie.proto.AgentConfig;
import com.netflix.genie.proto.AgentMetadata;
import com.netflix.genie.proto.ChangeJobArchiveStatusRequest;
import com.netflix.genie.proto.ChangeJobStatusRequest;
import com.netflix.genie.proto.ClaimJobRequest;
import com.netflix.genie.proto.ConfigureRequest;
import com.netflix.genie.proto.DryRunJobSpecificationRequest;
import com.netflix.genie.proto.ExecutionResource;
import com.netflix.genie.proto.GetJobStatusRequest;
import com.netflix.genie.proto.HandshakeRequest;
import com.netflix.genie.proto.JobSpecificationRequest;
import com.netflix.genie.proto.JobSpecificationResponse;
import com.netflix.genie.proto.ReserveJobIdRequest;
import org.apache.commons.lang3.StringUtils;

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
 * @see com.netflix.genie.common.internal.dtos.v4
 * @see com.netflix.genie.proto
 * @since 4.0.0
 */
public class JobServiceProtoConverter {

    /**
     * Convert a V4 Job Request DTO into a gRPC reserve job id request to be sent to the server.
     *
     * @param jobRequest          The job request to convert
     * @param agentClientMetadata The metadata about the agent
     * @return The request that should be sent to the server for a new Job Specification given the parameters
     * @throws GenieConversionException if conversion fails
     */
    public ReserveJobIdRequest toReserveJobIdRequestProto(
        final AgentJobRequest jobRequest,
        final AgentClientMetadata agentClientMetadata
    ) throws GenieConversionException {
        final ReserveJobIdRequest.Builder builder = ReserveJobIdRequest.newBuilder();
        builder.setMetadata(this.toJobMetadataProto(jobRequest));
        builder.setCriteria(this.toExecutionResourceCriteriaProto(jobRequest));
        builder.setAgentConfig(this.toAgentConfigProto(jobRequest.getRequestedAgentConfig()));
        builder.setAgentMetadata(this.toAgentMetadataProto(agentClientMetadata));
        return builder.build();
    }

    /**
     * Generate a {@link JobSpecificationRequest} from the given job id.
     *
     * @param id The job id to generate the request for
     * @return The request instance
     */
    public JobSpecificationRequest toJobSpecificationRequestProto(final String id) {
        return JobSpecificationRequest.newBuilder().setId(id).build();
    }

    /**
     * Convert a gRPC reserve job id request into a V4 Job Request DTO for use within Genie codebase.
     *
     * @param request The request to convert
     * @return The job request
     * @throws GenieConversionException if conversion fails
     */
    public JobRequest toJobRequestDto(final ReserveJobIdRequest request) throws GenieConversionException {
        return this.toJobRequestDto(
            request.getMetadata(),
            request.getCriteria(),
            request.getAgentConfig()
        );
    }

    /**
     * Convert a V4 Job Request DTO into a gRPC dry run resolve job specification request to be sent to the server.
     *
     * @param jobRequest The job request to convert
     * @return The request that should be sent to the server for a new Job Specification given the parameters
     * @throws GenieConversionException if conversion fails
     */
    public DryRunJobSpecificationRequest toDryRunJobSpecificationRequestProto(
        final AgentJobRequest jobRequest
    ) throws GenieConversionException {
        final DryRunJobSpecificationRequest.Builder builder = DryRunJobSpecificationRequest.newBuilder();
        builder.setMetadata(this.toJobMetadataProto(jobRequest));
        builder.setCriteria(this.toExecutionResourceCriteriaProto(jobRequest));
        builder.setAgentConfig(this.toAgentConfigProto(jobRequest.getRequestedAgentConfig()));
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
    public JobRequest toJobRequestDto(final DryRunJobSpecificationRequest request) throws GenieConversionException {
        return toJobRequestDto(
            request.getMetadata(),
            request.getCriteria(),
            request.getAgentConfig()
        );
    }

    /**
     * Convert a proto {@link AgentMetadata} to an {@link AgentClientMetadata}.
     *
     * @param agentMetadata The metadata to convert
     * @return The immutable DTO representation
     */
    public AgentClientMetadata toAgentClientMetadataDto(final AgentMetadata agentMetadata) {
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
    public JobSpecificationResponse toJobSpecificationResponseProto(final JobSpecification jobSpecification) {
        return JobSpecificationResponse
            .newBuilder()
            .setSpecification(this.toJobSpecificationProto(jobSpecification))
            .build();
    }

    /**
     * Convert a response from server into a Job Specification DTO which can be used in the codebase free of gRPC.
     *
     * @param protoSpec The protobuf specification message
     * @return A job specification DTO
     */
    public JobSpecification toJobSpecificationDto(
        final com.netflix.genie.proto.JobSpecification protoSpec
    ) {
        return new JobSpecification(
            protoSpec.getExecutableAndArgsList(),
            protoSpec.getJobArgsList(),
            toExecutionResourceDto(protoSpec.getJob()),
            toExecutionResourceDto(protoSpec.getCluster()),
            toExecutionResourceDto(protoSpec.getCommand()),
            protoSpec
                .getApplicationsList()
                .stream()
                .map(this::toExecutionResourceDto)
                .collect(Collectors.toList()),
            protoSpec.getEnvironmentVariablesMap(),
            protoSpec.getIsInteractive(),
            new File(protoSpec.getJobDirectoryLocation()),
            StringUtils.isBlank(protoSpec.getArchiveLocation()) ? null : protoSpec.getArchiveLocation(),
            protoSpec.hasTimeout() ? protoSpec.getTimeout().getValue() : null
        );
    }

    /**
     * Convert a Job Specification DTO to a protobuf message representation.
     *
     * @param jobSpecification The {@link JobSpecification} to convert
     * @return A {@link com.netflix.genie.proto.JobSpecification} instance
     */
    public com.netflix.genie.proto.JobSpecification toJobSpecificationProto(final JobSpecification jobSpecification) {
        final com.netflix.genie.proto.JobSpecification.Builder builder
            = com.netflix.genie.proto.JobSpecification.newBuilder();

        builder.addAllExecutableAndArgs(jobSpecification.getExecutableArgs());
        builder.addAllJobArgs(jobSpecification.getJobArgs());
        // Keep populating commandArgs for backward compatibility
        builder.addAllCommandArgs(jobSpecification.getExecutableArgs());
        builder.addAllCommandArgs(jobSpecification.getJobArgs());
        builder.setJob(toExecutionResourceProto(jobSpecification.getJob()));
        builder.setCluster(toExecutionResourceProto(jobSpecification.getCluster()));
        builder.setCommand(toExecutionResourceProto(jobSpecification.getCommand()));
        builder.addAllApplications(
            jobSpecification
                .getApplications()
                .stream()
                .map(this::toExecutionResourceProto)
                .collect(Collectors.toList())
        );
        builder.putAllEnvironmentVariables(jobSpecification.getEnvironmentVariables());
        builder.setIsInteractive(jobSpecification.isInteractive());
        builder.setJobDirectoryLocation(jobSpecification.getJobDirectoryLocation().getAbsolutePath());
        jobSpecification.getArchiveLocation().ifPresent(builder::setArchiveLocation);
        jobSpecification.getTimeout().ifPresent(timeout -> builder.setTimeout(Int32Value.of(timeout)));
        return builder.build();
    }

    /**
     * Convert agent metadata and job id into a ClaimJobRequest for the server.
     *
     * @param jobId               job id
     * @param agentClientMetadata agent metadata
     * @return a ClaimJobRequest
     */
    public ClaimJobRequest toClaimJobRequestProto(
        final String jobId,
        final AgentClientMetadata agentClientMetadata
    ) {
        return ClaimJobRequest.newBuilder()
            .setId(jobId)
            .setAgentMetadata(
                toAgentMetadataProto(agentClientMetadata)
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
    public ChangeJobStatusRequest toChangeJobStatusRequestProto(
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

    /**
     * Convert parameters into HandshakeRequest for the server.
     *
     * @param agentClientMetadata agent client metadata
     * @return a {@link HandshakeRequest}
     * @throws GenieConversionException if the inputs are invalid
     */
    public HandshakeRequest toHandshakeRequestProto(
        final AgentClientMetadata agentClientMetadata
    ) throws GenieConversionException {
        return HandshakeRequest.newBuilder()
            .setAgentMetadata(
                toAgentMetadataProto(agentClientMetadata)
            )
            .build();
    }

    /**
     * Convert parameters into ConfigureRequest for the server.
     *
     * @param agentClientMetadata agent client metadata
     * @return a {@link ConfigureRequest}
     * @throws GenieConversionException if the inputs are invalid
     */
    public ConfigureRequest toConfigureRequestProto(
        final AgentClientMetadata agentClientMetadata
    ) throws GenieConversionException {
        return ConfigureRequest.newBuilder()
            .setAgentMetadata(
                toAgentMetadataProto(agentClientMetadata)
            )
            .build();
    }

    /**
     * Convert a protobuf Agent config request to a DTO representation.
     *
     * @param protoAgentConfig The {@link AgentConfig} proto message to convert
     * @return A {@link AgentConfigRequest} instance with the necessary information
     */
    AgentConfigRequest toAgentConfigRequestDto(final AgentConfig protoAgentConfig) {
        return new AgentConfigRequest
            .Builder()
            .withRequestedJobDirectoryLocation(
                StringUtils.isNotBlank(protoAgentConfig.getJobDirectoryLocation())
                    ? protoAgentConfig.getJobDirectoryLocation()
                    : null
            )
            .withInteractive(protoAgentConfig.getIsInteractive())
            .withTimeoutRequested(protoAgentConfig.hasTimeout() ? protoAgentConfig.getTimeout().getValue() : null)
            .withArchivingDisabled(protoAgentConfig.getArchivingDisabled())
            .build();
    }

    /**
     * Convert a Agent configuration request DTO to a protobuf message representation.
     *
     * @param agentConfigRequest The {@link AgentConfigRequest} DTO to convert
     * @return A {@link AgentConfig} message instance
     */
    AgentConfig toAgentConfigProto(final AgentConfigRequest agentConfigRequest) {
        final AgentConfig.Builder builder = AgentConfig.newBuilder();
        agentConfigRequest.getRequestedJobDirectoryLocation().ifPresent(
            location -> builder.setJobDirectoryLocation(location.getAbsolutePath())
        );
        builder.setIsInteractive(agentConfigRequest.isInteractive());
        agentConfigRequest
            .getTimeoutRequested()
            .ifPresent(requestedTimeout -> builder.setTimeout(Int32Value.of(requestedTimeout)));
        builder.setArchivingDisabled(agentConfigRequest.isArchivingDisabled());
        return builder.build();
    }

    /**
     * Creates a request to fetch the job status currently seen by the server.
     *
     * @param jobId the job id
     * @return A {@link GetJobStatusRequest} message instance
     */
    public GetJobStatusRequest toGetJobStatusRequestProto(final String jobId) {
        return GetJobStatusRequest.newBuilder().setId(jobId).build();
    }

    /**
     * Creates a request to change the remote job archive status.
     *
     * @param jobId         the job id
     * @param archiveStatus the new archive status
     * @return a {@link ChangeJobArchiveStatusRequest} message instance
     */
    public ChangeJobArchiveStatusRequest toChangeJobStatusArchiveRequestProto(
        final String jobId,
        final ArchiveStatus archiveStatus
    ) {
        return ChangeJobArchiveStatusRequest.newBuilder()
            .setId(jobId)
            .setNewStatus(archiveStatus.name())
            .build();
    }

    private JobSpecification.ExecutionResource toExecutionResourceDto(final ExecutionResource protoResource) {
        return new JobSpecification.ExecutionResource(
            protoResource.getId(),
            new ExecutionEnvironment(
                ImmutableSet.copyOf(protoResource.getConfigsList()),
                ImmutableSet.copyOf(protoResource.getDependenciesList()),
                protoResource.getSetupFile().isEmpty() ? null : protoResource.getSetupFile()
            )
        );
    }

    private ExecutionResource toExecutionResourceProto(
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

    private Criterion toCriterionDto(
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
        } catch (final IllegalArgumentException e) {
            throw new GenieConversionException("Failed to convert criterion", e);
        }
    }

    private com.netflix.genie.proto.Criterion toCriterionProto(final Criterion criterion) {
        final com.netflix.genie.proto.Criterion.Builder builder = com.netflix.genie.proto.Criterion.newBuilder();
        criterion.getId().ifPresent(builder::setId);
        criterion.getName().ifPresent(builder::setName);
        criterion.getVersion().ifPresent(builder::setVersion);
        criterion.getStatus().ifPresent(builder::setStatus);
        builder.addAllTags(criterion.getTags());
        return builder.build();
    }

    private com.netflix.genie.proto.JobMetadata toJobMetadataProto(
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

    private com.netflix.genie.proto.ExecutionResourceCriteria toExecutionResourceCriteriaProto(
        final AgentJobRequest jobRequest
    ) {
        final ExecutionResourceCriteria executionResourceCriteria = jobRequest.getCriteria();
        final com.netflix.genie.proto.ExecutionResourceCriteria.Builder builder
            = com.netflix.genie.proto.ExecutionResourceCriteria.newBuilder();
        builder.addAllClusterCriteria(
            executionResourceCriteria
                .getClusterCriteria()
                .stream()
                .map(this::toCriterionProto)
                .collect(Collectors.toList())
        );
        builder.setCommandCriterion(
            toCriterionProto(executionResourceCriteria.getCommandCriterion())
        );
        builder.addAllRequestedApplicationIdOverrides(executionResourceCriteria.getApplicationIds());
        return builder.build();
    }

    private AgentMetadata toAgentMetadataProto(final AgentClientMetadata agentClientMetadata) {
        final AgentMetadata.Builder builder = AgentMetadata.newBuilder();
        agentClientMetadata.getHostname().ifPresent(builder::setAgentHostname);
        agentClientMetadata.getVersion().ifPresent(builder::setAgentVersion);
        agentClientMetadata.getPid().ifPresent(builder::setAgentPid);
        return builder.build();
    }

    private JobRequest toJobRequestDto(
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
                clusterCriteria.add(toCriterionDto(protoCriterion));
            }

            final ExecutionResourceCriteria executionResourceCriteria = new ExecutionResourceCriteria(
                clusterCriteria,
                toCriterionDto(protoExecutionResourceCriteria.getCommandCriterion()),
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

            final AgentConfigRequest agentConfigRequest = this.toAgentConfigRequestDto(protoAgentConfig);

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
        } catch (final IllegalArgumentException e) {
            throw new GenieConversionException("Failed to compose JobRequest", e);
        }
    }
}
