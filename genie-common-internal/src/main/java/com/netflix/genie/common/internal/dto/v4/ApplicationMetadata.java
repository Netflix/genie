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
import com.netflix.genie.common.dto.ApplicationStatus;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Optional;

/**
 * Metadata supplied by a user for an Application resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = ApplicationMetadata.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class ApplicationMetadata extends CommonMetadata {

    @Size(max = 255, message = "Max length of an application type is 255 characters")
    private final String type;
    @NotNull(message = "An application status is required")
    private final ApplicationStatus status;

    private ApplicationMetadata(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.type = builder.bType;
    }

    /**
     * Get the type of this application.
     *
     * @return The type wrapped in an {@link Optional}
     */
    public Optional<String> getType() {
        return Optional.ofNullable(this.type);
    }


    /**
     * A builder to create application user metadata instances.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonMetadata.Builder<Builder> {

        private final ApplicationStatus bStatus;
        private String bType;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the application
         * @param user    The user who owns the application
         * @param version The version of the application
         * @param status  The status of the application
         */
        @JsonCreator
        public Builder(
            @JsonProperty(value = "name", required = true) final String name,
            @JsonProperty(value = "user", required = true) final String user,
            @JsonProperty(value = "version", required = true) final String version,
            @JsonProperty(value = "status", required = true) final ApplicationStatus status
        ) {
            super(name, user, version);
            this.bStatus = status;
        }

        /**
         * Set the type of this application resource.
         *
         * @param type The type (e.g. Hadoop, Spark, etc) for grouping applications
         * @return The builder
         */
        public Builder withType(@Nullable final String type) {
            this.bType = StringUtils.isBlank(type) ? null : type;
            return this;
        }

        /**
         * Build the application metadata instance.
         *
         * @return Create the final read-only ApplicationMetadata instance
         */
        public ApplicationMetadata build() {
            return new ApplicationMetadata(this);
        }
    }
}
