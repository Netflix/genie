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

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Cluster DTO object. Read only after construction.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@JsonDeserialize(builder = Cluster.Builder.class)
public class Cluster extends ConfigDTO {

    private static final long serialVersionUID = 8562447832504925029L;

    @NotNull(message = "A valid cluster status is required")
    private final ClusterStatus status;
    private final Set<String> dependencies = new HashSet<>();

    /**
     * Constructor used only by the build() method of the builder.
     *
     * @param builder The builder to get data from
     */
    protected Cluster(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.dependencies.addAll(builder.bDependencies);
    }

    /**
     * Get the set of dependencies for the cluster.
     *
     * @return The dependencies for the cluster as a read-only set.
     */
    public Set<String> getDependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    /**
     * A builder to create clusters.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ConfigDTO.Builder<Builder> {

        private final ClusterStatus bStatus;
        private final Set<String> bDependencies = new HashSet<>();

        /**
         * Constructor which has required fields.
         *
         * @param name        The name to use for the Cluster
         * @param user        The user to use for the Cluster
         * @param version     The version to use for the Cluster
         * @param status      The status of the Cluster
         */
        public Builder(
            @JsonProperty("name")
            final String name,
            @JsonProperty("user")
            final String user,
            @JsonProperty("version")
            final String version,
            @JsonProperty("status")
            final ClusterStatus status
        ) {
            super(name, user, version);
            if (status != null) {
                this.bStatus = status;
            } else {
                this.bStatus = ClusterStatus.OUT_OF_SERVICE;
            }
        }

        /**
         * Set the dependencies for the cluster if desired.
         *
         * @param dependencies The dependencies
         * @return The builder
         */
        public Cluster.Builder withDependencies(final Set<String> dependencies) {
            if (dependencies != null) {
                this.bDependencies.addAll(dependencies);
            }
            return this;
        }

        /**
         * Build the cluster.
         *
         * @return Create the final read-only Cluster instance
         */
        public Cluster build() {
            return new Cluster(this);
        }
    }
}
