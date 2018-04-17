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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.Email;
import javax.validation.constraints.Size;
import java.util.Optional;

/**
 * Metadata supplied by a user for a job.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = JobMetadata.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class JobMetadata extends CommonMetadata {
    static final String DEFAULT_VERSION = "0.1";

    @Size(max = 255, message = "Max length of the group is 255 characters")
    private final String group;
    @Size(max = 255, message = "Max length of the email 255 characters")
    @Email(message = "Must be a valid email address")
    private final String email;
    private final String grouping;
    private final String groupingInstance;

    private JobMetadata(final Builder builder) {
        super(builder);
        this.group = builder.bGroup;
        this.email = builder.bEmail;
        this.grouping = builder.bGrouping;
        this.groupingInstance = builder.bGroupingInstance;
    }

    /**
     * Get the group the user should is a member of as an {@link Optional}.
     *
     * @return The group as an optional
     */
    public Optional<String> getGroup() {
        return Optional.ofNullable(this.group);
    }

    /**
     * Get the email for the user if there is one as an {@link Optional}.
     *
     * @return The email address as an Optional
     */
    public Optional<String> getEmail() {
        return Optional.ofNullable(this.email);
    }

    /**
     * Get the grouping for this job if there currently is one as an {@link Optional}.
     *
     * @return The grouping
     */
    public Optional<String> getGrouping() {
        return Optional.ofNullable(this.grouping);
    }

    /**
     * Get the grouping instance for this job if there currently is one as an {@link Optional}.
     *
     * @return The grouping instance
     */
    public Optional<String> getGroupingInstance() {
        return Optional.ofNullable(this.groupingInstance);
    }


    /**
     * A builder to create job user metadata instances.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonMetadata.Builder<Builder> {

        private String bGroup;
        private String bEmail;
        private String bGrouping;
        private String bGroupingInstance;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the job
         * @param user    The user to use for the Job
         * @param version The version of the job
         */
        @JsonCreator
        public Builder(
            @JsonProperty(value = "name", required = true) final String name,
            @JsonProperty(value = "user", required = true) final String user,
            @JsonProperty(value = "version", required = true) final String version
        ) {
            super(name, user, version);
        }

        /**
         * Constructor which will set a default version of 0.1.
         *
         * @param name The name to use for the job
         * @param user The user to use for the job
         */
        public Builder(final String name, final String user) {
            this(name, user, DEFAULT_VERSION);
        }

        /**
         * Set the group for the job.
         *
         * @param group The group
         * @return The builder
         */
        public Builder withGroup(@Nullable final String group) {
            this.bGroup = StringUtils.isBlank(group) ? null : group;
            return this;
        }

        /**
         * Set the email to use for alerting of job completion. If no alert desired leave blank.
         *
         * @param email the email address to use
         * @return The builder
         */
        public Builder withEmail(@Nullable final String email) {
            this.bEmail = StringUtils.isBlank(email) ? null : email;
            return this;
        }

        /**
         * Set the grouping to use for this job.
         *
         * @param grouping The grouping
         * @return The builder
         * @since 3.3.0
         */
        public Builder withGrouping(@Nullable final String grouping) {
            this.bGrouping = StringUtils.isBlank(grouping) ? null : grouping;
            return this;
        }

        /**
         * Set the grouping instance to use for this job.
         *
         * @param groupingInstance The grouping instance
         * @return The builder
         * @since 3.3.0
         */
        public Builder withGroupingInstance(@Nullable final String groupingInstance) {
            this.bGroupingInstance = StringUtils.isBlank(groupingInstance) ? null : groupingInstance;
            return this;
        }

        /**
         * Build the job user metadata instance.
         *
         * @return Create the final read-only JobMetadata instance
         */
        public JobMetadata build() {
            return new JobMetadata(this);
        }
    }
}
