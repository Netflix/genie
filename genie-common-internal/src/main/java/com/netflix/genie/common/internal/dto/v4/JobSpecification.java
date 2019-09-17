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

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private final ImmutableList<String> executableArgs;
    private final ImmutableList<String> jobArgs;
    private final ExecutionResource job;
    private final ExecutionResource cluster;
    private final ExecutionResource command;
    private final ImmutableList<ExecutionResource> applications;
    private final ImmutableMap<String, String> environmentVariables;
    private final boolean interactive;
    private final File jobDirectoryLocation;
    private final String archiveLocation;
    private final Integer timeout;

    /**
     * Constructor.
     *
     * @param executableArgs       Executable and its fixed argument provided by the Command
     * @param jobArgs              Job arguments provided by the user for this job
     * @param job                  The execution resources for a specific job
     * @param cluster              The execution resources for a specific cluster used for a job
     * @param command              The execution resources for a specific command used for a job
     * @param applications         The execution resources of all applications used for a job. Optional
     * @param environmentVariables The environment variables the agent should set when running the job. Optional
     * @param interactive          Whether the job is interactive or not
     * @param jobDirectoryLocation Location on disk where the job directory will be created
     * @param archiveLocation      Location where job folder is archived by the agent when job finishes. Optional
     * @param timeout              The number of seconds after a job starts that it should be killed due to timeout.
     *                             Optional
     */
    @JsonCreator
    public JobSpecification(
        @JsonProperty("executableArgs") @Nullable final List<String> executableArgs,
        @JsonProperty("jobArgs") @Nullable final List<String> jobArgs,
        @JsonProperty(value = "job", required = true) final ExecutionResource job,
        @JsonProperty(value = "cluster", required = true) final ExecutionResource cluster,
        @JsonProperty(value = "command", required = true) final ExecutionResource command,
        @JsonProperty("applications") @Nullable final List<ExecutionResource> applications,
        @JsonProperty("environmentVariables") @Nullable final Map<String, String> environmentVariables,
        @JsonProperty(value = "interactive", required = true) final boolean interactive,
        @JsonProperty(value = "jobDirectoryLocation", required = true) final File jobDirectoryLocation,
        @JsonProperty(value = "archiveLocation") @Nullable final String archiveLocation,
        @JsonProperty(value = "timeout") @Nullable final Integer timeout
    ) {

        this.executableArgs = executableArgs == null ? ImmutableList.of() : ImmutableList.copyOf(executableArgs);
        this.jobArgs = jobArgs == null ? ImmutableList.of() : ImmutableList.copyOf(jobArgs);
        this.job = job;
        this.cluster = cluster;
        this.command = command;
        this.applications = applications == null ? ImmutableList.of() : ImmutableList.copyOf(applications);
        this.environmentVariables = environmentVariables == null
            ? ImmutableMap.of()
            : ImmutableMap.copyOf(environmentVariables);
        this.interactive = interactive;
        this.jobDirectoryLocation = jobDirectoryLocation;
        this.archiveLocation = archiveLocation;
        this.timeout = timeout;
    }

    /**
     * Returns an unmodifiable list of executable and arguments provided by the Command resolved to.
     *
     * @return A list of executable and arguments that will throw exception if modifications are attempted
     */
    public List<String> getExecutableArgs() {
        return executableArgs;
    }

    /**
     * Returns an unmodifiable list of arguments provided by the user for this job.
     *
     * @return A list of arguments that will throw exception if modifications are attempted
     */
    public List<String> getJobArgs() {
        return jobArgs;
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
     * Get the archive location for the job folder.
     *
     * @return archive location for the job folder wrapped in an {@link Optional}
     */
    public Optional<String> getArchiveLocation() {
        return Optional.ofNullable(this.archiveLocation);
    }

    /**
     * Get the job timeout.
     *
     * @return The number of seconds after a job launch that this job should be killed by the agent due to timeout.
     * Wrapped in {@link Optional} as it's not required. {@link Optional#empty()} means there is no timeout and the job
     * can run indefinitely.
     */
    public Optional<Integer> getTimeout() {
        return Optional.ofNullable(this.timeout);
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
