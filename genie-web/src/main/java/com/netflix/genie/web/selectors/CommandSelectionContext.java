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

import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import lombok.Getter;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

/**
 * Extension of {@link ResourceSelectionContext} to include specific data useful in command selection.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
public class CommandSelectionContext extends ResourceSelectionContext<Command> {

    private final Map<Command, Set<Cluster>> commandToClusters;

    /**
     * Constructor.
     *
     * @param jobId             The id of the job which the command is being selected for
     * @param jobRequest        The job request the user originally made
     * @param apiJob            Whether the job was submitted via the API or from Agent CLI
     * @param commandToClusters The map of command candidates to their respective clusters candidates
     */
    public CommandSelectionContext(
        @NotBlank final String jobId,
        @NotNull final JobRequest jobRequest,
        final boolean apiJob,
        @NotEmpty final Map<@Valid Command, @NotEmpty Set<@Valid Cluster>> commandToClusters
    ) {
        super(jobId, jobRequest, apiJob);
        this.commandToClusters = ImmutableMap.copyOf(commandToClusters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Command> getResources() {
        return this.commandToClusters.keySet();
    }
}
