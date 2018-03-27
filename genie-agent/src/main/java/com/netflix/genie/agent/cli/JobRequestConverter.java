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

import com.netflix.genie.common.dto.v4.AgentJobRequest;
import com.netflix.genie.common.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.dto.v4.JobMetadata;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Convert job request arguments delegate into an AgentJobRequest.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@NoArgsConstructor
public class JobRequestConverter {

    /**
     * Convert Job request arguments into an AgentJobRequest object.
     * @param jobRequestArguments a job request arguments delegate
     * @return an AgentJobRequest DTO
     */
    public AgentJobRequest agentJobRequestArgsToDTO(
        final ArgumentDelegates.JobRequestArguments jobRequestArguments
    ) {
        final ExecutionResourceCriteria criteria = new ExecutionResourceCriteria(
            jobRequestArguments.getClusterCriteria(),
            jobRequestArguments.getCommandCriterion(),
            jobRequestArguments.getApplicationIds()
        );

        final JobMetadata.Builder jobMetadataBuilder = new JobMetadata.Builder(
            jobRequestArguments.getJobName(),
            jobRequestArguments.getUser()
        )
            .withEmail(jobRequestArguments.getEmail())
            .withGrouping(jobRequestArguments.getGrouping())
            .withGroupingInstance(jobRequestArguments.getGroupingInstance())
            .withMetadata(jobRequestArguments.getJobMetadata())
            .withDescription(jobRequestArguments.getJobDescription())
            .withTags(jobRequestArguments.getJobTags())
            .withVersion(jobRequestArguments.getJobVersion());

        final JobMetadata jobMetadata = jobMetadataBuilder.build();

        return new AgentJobRequest.Builder(
            jobMetadata,
            criteria,
            jobRequestArguments.getJobDirectoryLocation().getAbsolutePath()
        )
            .withCommandArgs(jobRequestArguments.getCommandArguments())
            .withInteractive(jobRequestArguments.isInteractive())
            .withArchivingDisabled(jobRequestArguments.isArchivalDisabled())
            .withTimeout(jobRequestArguments.getTimeout())
            .withRequestedId(jobRequestArguments.getJobId())
            .build();
    }
}
