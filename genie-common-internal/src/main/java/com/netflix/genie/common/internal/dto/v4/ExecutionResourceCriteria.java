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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Container for various options for user supplying criteria for the execution environment of a job.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public class ExecutionResourceCriteria {

    @NotEmpty(message = "At least one cluster criterion is required")
    private final ImmutableList<@Valid Criterion> clusterCriteria;
    @NotNull(message = "Command criterion is required")
    @Valid
    private final Criterion commandCriterion;
    private final ImmutableList<String> applicationIds;

    /**
     * Constructor.
     *
     * @param clusterCriteria  The ordered list of criteria used to find a cluster for job execution. Not null/empty.
     * @param commandCriterion The command criterion used to find a command to run on the cluster for job execution.
     *                         Not null.
     * @param applicationIds   The ordered list of application ids to override the applications associated with
     *                         selected command for job execution. Optional. Any blanks will be removed
     */
    @JsonCreator
    public ExecutionResourceCriteria(
        @JsonProperty(value = "clusterCriteria", required = true) final List<Criterion> clusterCriteria,
        @JsonProperty(value = "commandCriterion", required = true) final Criterion commandCriterion,
        @JsonProperty("applicationIds") @Nullable final List<String> applicationIds
    ) {
        this.clusterCriteria = ImmutableList.copyOf(clusterCriteria);
        this.commandCriterion = commandCriterion;
        this.applicationIds = applicationIds == null ? ImmutableList.of() : ImmutableList.copyOf(
            applicationIds
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList())
        );
    }

    /**
     * Get the ordered list of criteria the system should use to find a cluster for job execution. The underlying
     * implementation is immutable and any attempt to modify will throw an exception.
     *
     * @return The list of criterion in the order they should be evaluated along with supplied command criterion
     */
    public List<Criterion> getClusterCriteria() {
        return this.clusterCriteria;
    }

    /**
     * Get the ordered list of ids the user desires to override the applications associated with selected command with.
     * This list is immutable and any attempt to modify will throw an exception.
     *
     * @return The ids in the order they should be setup by the execution module
     */
    public List<String> getApplicationIds() {
        return this.applicationIds;
    }
}
