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
package com.netflix.genie.agent.cli;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.constraints.NotEmpty;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Convert job request arguments delegate into an {@link AgentJobRequest}.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class JobRequestConverter {

    private static final String SINGLE_QUOTE = "'";
    private static final String ESCAPED_SINGLE_QUOTE = "'\\''";
    private static final String SPACE = " ";
    private final Validator validator;

    JobRequestConverter(final Validator validator) {
        this.validator = validator;
    }

    /**
     * Convert Job request arguments into an AgentJobRequest object.
     *
     * @param jobRequestArguments a job request arguments delegate
     * @return an AgentJobRequest DTO
     * @throws ConversionException if the resulting AgentJobRequest fails validation
     */
    public AgentJobRequest agentJobRequestArgsToDTO(
        final ArgumentDelegates.JobRequestArguments jobRequestArguments
    ) throws ConversionException {
        final ExecutionResourceCriteria criteria = new ExecutionResourceCriteria(
            jobRequestArguments.getClusterCriteria(),
            jobRequestArguments.getCommandCriterion(),
            jobRequestArguments.getApplicationIds()
        );

        final String jobVersion = jobRequestArguments.getJobVersion();
        final JobMetadata.Builder jobMetadataBuilder;
        if (StringUtils.isBlank(jobVersion)) {
            jobMetadataBuilder = new JobMetadata.Builder(
                jobRequestArguments.getJobName(),
                jobRequestArguments.getUser()
            );
        } else {
            jobMetadataBuilder = new JobMetadata.Builder(
                jobRequestArguments.getJobName(),
                jobRequestArguments.getUser(),
                jobVersion
            );
        }

        jobMetadataBuilder
            .withEmail(jobRequestArguments.getEmail())
            .withGrouping(jobRequestArguments.getGrouping())
            .withGroupingInstance(jobRequestArguments.getGroupingInstance())
            .withMetadata(jobRequestArguments.getJobMetadata())
            .withDescription(jobRequestArguments.getJobDescription())
            .withTags(jobRequestArguments.getJobTags())
            .build();

        final AgentConfigRequest requestedAgentConfig = new AgentConfigRequest
            .Builder()
            .withRequestedJobDirectoryLocation(jobRequestArguments.getJobDirectoryLocation())
            .withTimeoutRequested(jobRequestArguments.getTimeout())
            .withInteractive(jobRequestArguments.isInteractive())
            .withArchivingDisabled(jobRequestArguments.isArchivingDisabled())
            .build();

        final List<String> configs = jobRequestArguments.getJobConfigurations();
        final List<String> deps = jobRequestArguments.getJobDependencies();
        final ExecutionEnvironment jobExecutionResources = new ExecutionEnvironment(
            configs.isEmpty() ? null : Sets.newHashSet(configs),
            deps.isEmpty() ? null : Sets.newHashSet(deps),
            jobRequestArguments.getJobSetup()
        );

        // Convert split, parsed arguments received via command-line back to the same single-string, unparsed
        // format that comes through the V3 API.
        final List<String> commandArguments = getV3ArgumentsString(jobRequestArguments.getCommandArguments());

        final AgentJobRequest agentJobRequest = new AgentJobRequest.Builder(
            jobMetadataBuilder.build(),
            criteria,
            requestedAgentConfig
        )
            .withCommandArgs(commandArguments)
            .withRequestedId(jobRequestArguments.getJobId())
            .withResources(jobExecutionResources)
            .build();

        final Set<ConstraintViolation<AgentJobRequest>> violations = this.validator.validate(agentJobRequest);

        if (!violations.isEmpty()) {
            throw new ConversionException(violations);
        }

        return agentJobRequest;
    }

    // In order to make all command arguments look the same in a job specification object, transform the
    // split and unwrapped arguments back into a quoted string.
    // This allows the downstream agent code to make no special case.
    // All arguments are coming in as they would from the V3 API.
    // This means:
    // - Escape single-quotes
    // - Wrap each token in single quotes
    // - Join everything into a single string and send it as a list with just one element
    private List<String> getV3ArgumentsString(final List<String> commandArguments) {
        return Lists.newArrayList(
            commandArguments.stream()
                .map(s -> s.replaceAll(SINGLE_QUOTE, Matcher.quoteReplacement(ESCAPED_SINGLE_QUOTE)))
                .map(s -> SINGLE_QUOTE + s + SINGLE_QUOTE)
                .collect(Collectors.joining(SPACE))
        );
    }

    /**
     * Exception thrown in case of conversion error due to resulting object failing validation.
     */
    public static class ConversionException extends Exception {

        @Getter
        private final Set<ConstraintViolation<AgentJobRequest>> violations;

        ConversionException(@NotEmpty final Set<ConstraintViolation<AgentJobRequest>> violations) {
            super(
                String.format(
                    "Job request failed validation: %s (%d total violations)",
                    violations.iterator().next().getMessage(),
                    violations.size()
                )
            );
            this.violations = violations;
        }
    }
}
