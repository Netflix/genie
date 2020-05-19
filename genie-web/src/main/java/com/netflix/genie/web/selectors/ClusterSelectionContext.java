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
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Optional;
import java.util.Set;

/**
 * Extension of {@link ResourceSelectionContext} to include specific data useful in cluster selection.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
public class ClusterSelectionContext extends ResourceSelectionContext<Cluster> {

    private final Command command;
    private final Set<Cluster> clusters;

    /**
     * Constructor.
     *
     * @param jobId      The id of the job which the command is being selected for
     * @param jobRequest The job request the user originally made
     * @param apiJob     Whether the job was submitted via the API or from Agent CLI
     * @param command    The command which was already selected (if there was one)
     * @param clusters   The clusters to choose from
     */
    public ClusterSelectionContext(
        @NotEmpty final String jobId,
        @NotNull final JobRequest jobRequest,
        final boolean apiJob,
        @Nullable @Valid final Command command,
        @NotEmpty final Set<@Valid Cluster> clusters
    ) {
        super(jobId, jobRequest, apiJob);
        this.command = command;
        this.clusters = ImmutableSet.copyOf(clusters);
    }

    /**
     * Get the command which was already selected for the job if there was one.
     * <p>
     * This is currently returning an optional due to the support for v3 and v4 algorithms. Once v4 is the only
     * resource selection algorithm command will no longer be optional.
     *
     * @return The {@link Command} wrapped in an {@link Optional}
     */
    public Optional<Command> getCommand() {
        return Optional.ofNullable(this.command);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Cluster> getResources() {
        return this.clusters;
    }
}
