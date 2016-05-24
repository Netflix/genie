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
package com.netflix.genie.client.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;
import com.netflix.genie.common.dto.JobStatus;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Date;

/**
 * This class represents the subset of data returned from a Job when a search for Jobs is conducted.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobSearchResult extends com.netflix.genie.common.dto.search.JobSearchResult {

    private static final long serialVersionUID = -3886685874572773514L;

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
        @NotBlank @JsonProperty("id") final String id,
        @NotBlank @JsonProperty("name") final String name,
        @NotBlank @JsonProperty("user") final String user,
        @NotNull @JsonProperty("status") final JobStatus status,
        @JsonProperty("started") final Date started,
        @JsonProperty("finished") final Date finished,
        @JsonProperty("clusterName") final String clusterName,
        @JsonProperty("commandName") final String commandName,
        @JsonProperty("runtime") final Duration runtime
        ) {
        super(id, name, user, status, started, finished, clusterName, commandName);
        this.runtime = runtime;
    }
}
