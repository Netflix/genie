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
package com.netflix.genie.common.dto.v4;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.io.File;
import java.util.List;
import java.util.Optional;

/**
 * All details a user will provide to Genie in order to run a job.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true, doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class JobRequest extends CommonRequestImpl implements AgentJobRequest, ApiJobRequest {
    private final ImmutableList<
        @Size(
            max = 10_000,
            message = "Max length of an individual command line argument is 10,000 characters"
        ) String> commandArgs;
    private final boolean archivingDisabled;
    @Min(value = 1, message = "The timeout must be at least 1 second, preferably much more.")
    private final Integer timeout;
    private final boolean interactive;
    @Valid
    private final JobMetadata metadata;
    @Valid
    private final ExecutionResourceCriteria criteria;
    @Valid
    private final AgentEnvironmentRequest requestedAgentEnvironment;

    JobRequest(final AgentJobRequest.Builder builder) {
        this(
            builder.getBRequestedId(),
            builder.getBResources(),
            builder.getBCommandArgs(),
            builder.isBArchivingDisabled(),
            builder.getBTimeout(),
            builder.isBInteractive(),
            builder.getBMetadata(),
            builder.getBCriteria(),
            new AgentEnvironmentRequest
                .Builder()
                .withRequestedJobDirectoryLocation(builder.getBRequestedJobDirectoryLocation())
                .build()
        );
    }

    JobRequest(final ApiJobRequest.Builder builder) {
        this(
            builder.getBRequestedId(),
            builder.getBResources(),
            builder.getBCommandArgs(),
            builder.isBArchivingDisabled(),
            builder.getBTimeout(),
            builder.isBInteractive(),
            builder.getBMetadata(),
            builder.getBCriteria(),
            builder.getBRequestedAgentEnvironment()
        );
    }

    /**
     * Constructor.
     *
     * @param requestedId               The requested id of the job if one was provided by the user
     * @param resources                 The execution resources (if any) provided by the user
     * @param commandArgs               Any command args provided by the user
     * @param archivingDisabled         Whether to disable archiving of the job directory after job completion
     * @param timeout                   The timeout (in seconds) of the job
     * @param interactive               Whether the job is interactive or not
     * @param metadata                  Any metadata related to the job provided by the user
     * @param criteria                  The criteria used by the server to determine execution resources
     *                                  (cluster, command, etc)
     * @param requestedAgentEnvironment The optional agent environment request parameters
     */
    public JobRequest(
        @Nullable final String requestedId,
        @Nullable final ExecutionEnvironment resources,
        @Nullable final List<String> commandArgs,
        final boolean archivingDisabled,
        @Nullable final Integer timeout,
        final boolean interactive,
        final JobMetadata metadata,
        final ExecutionResourceCriteria criteria,
        @Nullable final AgentEnvironmentRequest requestedAgentEnvironment
    ) {
        super(requestedId, resources);
        this.commandArgs = commandArgs == null ? ImmutableList.of() : ImmutableList.copyOf(commandArgs);
        this.archivingDisabled = archivingDisabled;
        this.timeout = timeout;
        this.interactive = interactive;
        this.metadata = metadata;
        this.criteria = criteria;
        this.requestedAgentEnvironment = requestedAgentEnvironment == null
            ? new AgentEnvironmentRequest.Builder().build()
            : requestedAgentEnvironment;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getTimeout() {
        return Optional.ofNullable(this.timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<File> getRequestedJobDirectoryLocation() {
        return this.requestedAgentEnvironment.getRequestedJobDirectoryLocation();
    }
}
