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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Application DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = Application.Builder.class)
@Getter
public class Application extends ConfigDTO {

    private static final long serialVersionUID = 212266105066344180L;

    private final Set<String> dependencies = new HashSet<>();
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
        this.dependencies.addAll(builder.bDependencies);
        this.type = builder.bType;
    }

    /**
     * Get the set of dependencies for the application.
     *
     * @return The dependencies for the applicatoin as a read-only set.
     */
    public Set<String> getDependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    /**
     * A builder to create applications.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ConfigDTO.Builder<Builder> {

        private final ApplicationStatus bStatus;
        private final Set<String> bDependencies = new HashSet<>();
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
            @JsonProperty("name")
            final String name,
            @JsonProperty("user")
            final String user,
            @JsonProperty("version")
            final String version,
            @JsonProperty("status")
            final ApplicationStatus status
        ) {
            super(name, user, version);
            if (status != null) {
                this.bStatus = status;
            } else {
                this.bStatus = ApplicationStatus.INACTIVE;
            }
        }

        /**
         * Set the dependencies for the application if desired.
         *
         * @param dependencies The dependencies
         * @return The builder
         */
        public Builder withDependencies(final Set<String> dependencies) {
            if (dependencies != null) {
                this.bDependencies.addAll(dependencies);
            }
            return this;
        }

        /**
         * Set the type of this application.
         *
         * @param type The type (e.g. Hadoop, Spark, etc) for grouping applications
         * @return The builder
         */
        public Builder withType(final String type) {
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
