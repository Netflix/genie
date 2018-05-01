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
package com.netflix.genie.common.internal.dto.v4;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.stream.Collectors;

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
        @NotBlank(message = "A command argument shouldn't be a blank string")
        @Size(
            max = 10_000,
            message = "Max length of an individual command line argument is 10,000 characters"
        ) String>
        commandArgs;
    @Valid
    private final JobMetadata metadata;
    @Valid
    private final ExecutionResourceCriteria criteria;
    @Valid
    private final AgentEnvironmentRequest requestedAgentEnvironment;
    @Valid
    private final AgentConfigRequest requestedAgentConfig;

    JobRequest(final AgentJobRequest.Builder builder) {
        this(
            builder.getBRequestedId(),
            builder.getBResources(),
            builder.getBCommandArgs(),
            builder.getBMetadata(),
            builder.getBCriteria(),
            null,
            builder.getBRequestedAgentConfig()
        );
    }

    JobRequest(final ApiJobRequest.Builder builder) {
        this(
            builder.getBRequestedId(),
            builder.getBResources(),
            builder.getBCommandArgs(),
            builder.getBMetadata(),
            builder.getBCriteria(),
            builder.getBRequestedAgentEnvironment(),
            builder.getBRequestedAgentConfig()
        );
    }

    /**
     * Constructor.
     *
     * @param requestedId               The requested id of the job if one was provided by the user
     * @param resources                 The execution resources (if any) provided by the user
     * @param commandArgs               Any command args provided by the user
     * @param metadata                  Any metadata related to the job provided by the user
     * @param criteria                  The criteria used by the server to determine execution resources
     *                                  (cluster, command, etc)
     * @param requestedAgentEnvironment The optional agent environment request parameters
     * @param requestedAgentConfig      The optional configuration options for the Genie Agent
     */
    public JobRequest(
        @Nullable final String requestedId,
        @Nullable final ExecutionEnvironment resources,
        @Nullable final List<String> commandArgs,
        final JobMetadata metadata,
        final ExecutionResourceCriteria criteria,
        @Nullable final AgentEnvironmentRequest requestedAgentEnvironment,
        @Nullable final AgentConfigRequest requestedAgentConfig
    ) {
        super(requestedId, resources);
        this.commandArgs = commandArgs == null ? ImmutableList.of() : ImmutableList.copyOf(
            commandArgs
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList())
        );
        this.metadata = metadata;
        this.criteria = criteria;
        this.requestedAgentEnvironment = requestedAgentEnvironment == null
            ? new AgentEnvironmentRequest.Builder().build()
            : requestedAgentEnvironment;
        this.requestedAgentConfig = requestedAgentConfig == null
            ? new AgentConfigRequest.Builder().build()
            : requestedAgentConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getCommandArgs() {
        return this.commandArgs;
    }
}
