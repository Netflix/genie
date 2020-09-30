/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.selectors;

import com.google.common.collect.ImmutableSet;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import lombok.Getter;
import lombok.ToString;

import javax.validation.constraints.NotBlank;
import java.util.Collection;
import java.util.Set;

/**
 * Extension of {@link ResourceSelectionContext} to include specific data useful in AgentLauncher selection.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
public class AgentLauncherSelectionContext extends ResourceSelectionContext<AgentLauncher> {
    private final JobRequestMetadata jobRequestMetadata;
    private final Set<AgentLauncher> agentLaunchers;

    /**
     * Constructor.
     *
     * @param jobId              the job id
     * @param jobRequest         the job request
     * @param apiJob             whether this job was submitted through the API
     * @param jobRequestMetadata the job request metadata
     * @param agentLaunchers     the list of available launchers
     */
    public AgentLauncherSelectionContext(
        @NotBlank final String jobId,
        final JobRequest jobRequest,
        final boolean apiJob,
        final JobRequestMetadata jobRequestMetadata,
        final Collection<AgentLauncher> agentLaunchers
    ) {
        super(jobId, jobRequest, apiJob);
        this.jobRequestMetadata = jobRequestMetadata;
        this.agentLaunchers = ImmutableSet.copyOf(agentLaunchers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<AgentLauncher> getResources() {
        return agentLaunchers;
    }
}
