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
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This DTO represents all the information needed to execute a job by the Genie Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public class JobSpecification {

    private final ImmutableList<String> commandArgs;
    private final ExecutionResource job;
    private final ExecutionResource cluster;
    private final ExecutionResource command;
    private final ImmutableList<ExecutionResource> applications;
    private final ImmutableMap<String, String> environmentVariables;
    private final boolean interactive;
    private final File jobDirectoryLocation;

    /**
     * Constructor.
     *
     * @param commandArgs          Any command arguments for the job. Optional. Any blanks will be removed
     * @param job                  The execution resources for a specific job
     * @param cluster              The execution resources for a specific cluster used for a job
     * @param command              The execution resources for a specific command used for a job
     * @param applications         The execution resources of all applications used for a job. Optional
     * @param environmentVariables The environment variables the agent should set when running the job. Optional
     * @param interactive          Whether the job is interactive or not
     * @param jobDirectoryLocation Location on disk where the job directory will be created
     */
    @JsonCreator
    public JobSpecification(
        @JsonProperty("commandArgs") @Nullable final List<String> commandArgs,
        @JsonProperty(value = "job", required = true) final ExecutionResource job,
        @JsonProperty(value = "cluster", required = true) final ExecutionResource cluster,
        @JsonProperty(value = "command", required = true) final ExecutionResource command,
        @JsonProperty("applications") @Nullable final List<ExecutionResource> applications,
        @JsonProperty("environmentVariables") @Nullable final Map<String, String> environmentVariables,
        @JsonProperty(value = "interactive", required = true) final boolean interactive,
        @JsonProperty(value = "jobDirectoryLocation", required = true) final File jobDirectoryLocation
    ) {
        this.commandArgs = commandArgs == null ? ImmutableList.of() : ImmutableList.copyOf(
            commandArgs
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList())
        );
        this.job = job;
        this.cluster = cluster;
        this.command = command;
        this.applications = applications == null ? ImmutableList.of() : ImmutableList.copyOf(applications);
        this.environmentVariables = environmentVariables == null
            ? ImmutableMap.of()
            : ImmutableMap.copyOf(environmentVariables);
        this.interactive = interactive;
        this.jobDirectoryLocation = jobDirectoryLocation;
    }

    /**
     * Returns an unmodifiable list of the command args for this job specification.
     *
     * @return A view of the list of command args that will throw exception if modifications are attempted
     */
    public List<String> getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Returns an unmodifiable list of applications.
     *
     * @return A list of Applications that will throw exception if modifications are attempted
     */
    public List<ExecutionResource> getApplications() {
        return this.applications;
    }

    /**
     * Get the environment variables dictated by the server that should be set in the job execution environment.
     *
     * @return The variable name to value pairs that should be set in the execution environment of the job. This map
     * will be immutable and any attempt to modify will result in an exception
     */
    public Map<String, String> getEnvironmentVariables() {
        return this.environmentVariables;
    }

    /**
     * Common representation of resources used for job execution e.g. a Cluster, Command, Application.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Getter
    @EqualsAndHashCode(doNotUseGetters = true)
    @ToString(doNotUseGetters = true)
    public static class ExecutionResource {

        private final String id;
        private final ExecutionEnvironment executionEnvironment;

        /**
         * Constructor.
         *
         * @param id                   The unique identifier of this execution resource
         * @param executionEnvironment The environment that should be setup for this resource
         */
        @JsonCreator
        public ExecutionResource(
            @JsonProperty(value = "id", required = true) final String id,
            @JsonProperty(
                value = "executionEnvironment",
                required = true
            ) final ExecutionEnvironment executionEnvironment
        ) {
            this.id = id;
            this.executionEnvironment = executionEnvironment;
        }
    }
}
