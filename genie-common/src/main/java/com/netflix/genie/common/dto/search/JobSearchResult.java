/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.common.dto.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.util.TimeUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * This class represents the subset of data returned from a Job when a search for Jobs is conducted.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class JobSearchResult extends BaseSearchResult {

    private static final long serialVersionUID = -3886685874572773514L;

    private final JobStatus status;
    private final Instant started;
    private final Instant finished;
    private final String clusterName;
    private final String commandName;
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonDeserialize(using = DurationDeserializer.class)
    private final Duration runtime;

    /**
     * Constructor.
     *
     * @param id          The id of the job
     * @param name        The name of the job
     * @param user        The user of the job
     * @param status      The current status of the job
     * @param started     The start time of the job
     * @param finished    The finish time of the job
     * @param clusterName The name of the cluster this job is or was run on
     * @param commandName The name fo the command this job is or was run with
     */
    @JsonCreator
    public JobSearchResult(
        @JsonProperty("id") @NotBlank final String id,
        @JsonProperty("name") @NotBlank final String name,
        @JsonProperty("user") @NotBlank final String user,
        @JsonProperty("status") @NotNull final JobStatus status,
        @JsonProperty("started") @Nullable final Instant started,
        @JsonProperty("finished") @Nullable final Instant finished,
        @JsonProperty("clusterName") @Nullable final String clusterName,
        @JsonProperty("commandName") @Nullable final String commandName
    ) {
        super(id, name, user);
        this.status = status;
        this.started = started;
        this.finished = finished;
        this.clusterName = clusterName;
        this.commandName = commandName;

        this.runtime = TimeUtils.getDuration(this.started, this.finished);
    }

    /**
     * Get the time the job started.
     *
     * @return The started time or null if not set
     */
    public Optional<Instant> getStarted() {
        return Optional.ofNullable(this.started);
    }

    /**
     * Get the time the job finished.
     *
     * @return The finished time or null if not set
     */
    public Optional<Instant> getFinished() {
        return Optional.ofNullable(this.finished);
    }

    /**
     * Get the name of the cluster running the job if there currently is one.
     *
     * @return The name of the cluster where the job is running
     */
    public Optional<String> getClusterName() {
        return Optional.ofNullable(this.clusterName);
    }

    /**
     * Get the name of the command running this job if there currently is one.
     *
     * @return The name of the command
     */
    public Optional<String> getCommandName() {
        return Optional.ofNullable(this.commandName);
    }
}
