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

import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest;
import com.netflix.genie.common.internal.dto.v4.AgentJobRequest;
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.constraints.NotEmpty;
import java.util.Set;

/**
 * Convert job request arguments delegate into an AgentJobRequest.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
public class JobRequestConverter {

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
            .withArchivingDisabled(jobRequestArguments.isArchivalDisabled())
            .withInteractive(jobRequestArguments.isInteractive())
            .build();

        final AgentJobRequest agentJobRequest = new AgentJobRequest.Builder(
            jobMetadataBuilder.build(),
            criteria,
            requestedAgentConfig
        )
            .withCommandArgs(jobRequestArguments.getCommandArguments())
            .withRequestedId(jobRequestArguments.getJobId())
            .build();

        final Set<ConstraintViolation<AgentJobRequest>> violations = this.validator.validate(agentJobRequest);

        if (!violations.isEmpty()) {
            throw new ConversionException(violations);
        }

        return agentJobRequest;
    }

    /**
     * Exception thrown in case of conversion error due to resulting object failing validation.
     */
    public static final class ConversionException extends Exception {

        @Getter
        private final Set<ConstraintViolation<AgentJobRequest>> violations;

        private ConversionException(@NotEmpty final Set<ConstraintViolation<AgentJobRequest>> violations) {
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
