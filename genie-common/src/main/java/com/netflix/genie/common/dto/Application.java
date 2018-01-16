/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Optional;

/**
 * Application DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@JsonDeserialize(builder = Application.Builder.class)
public class Application extends ExecutionEnvironmentDTO {

    private static final long serialVersionUID = 212266105066344180L;

    @NotNull(message = "An application status is required")
    private final ApplicationStatus status;
    private final String type;

    /**
     * Constructor only accessible via builder build() method.
     *
     * @param builder The builder to get data from
     */
    protected Application(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.type = builder.bType;
    }

    /**
     * Get the type of this application.
     *
     * @return The type as an Optional
     */
    public Optional<String> getType() {
        return Optional.ofNullable(this.type);
    }

    /**
     * A builder to create applications.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ExecutionEnvironmentDTO.Builder<Builder> {

        private final ApplicationStatus bStatus;
        private String bType;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the Application
         * @param user    The user to use for the Application
         * @param version The version to use for the Application
         * @param status  The status of the Application
         */
        public Builder(
            @JsonProperty("name") final String name,
            @JsonProperty("user") final String user,
            @JsonProperty("version") final String version,
            @JsonProperty("status") final ApplicationStatus status
        ) {
            super(name, user, version);
            this.bStatus = status;
        }

        /**
         * Set the type of this application.
         *
         * @param type The type (e.g. Hadoop, Spark, etc) for grouping applications
         * @return The builder
         */
        public Builder withType(@Nullable final String type) {
            this.bType = type;
            return this;
        }

        /**
         * Build the application.
         *
         * @return Create the final read-only Application instance
         */
        public Application build() {
            return new Application(this);
        }
    }
}
