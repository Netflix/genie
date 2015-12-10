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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * Cluster DTO object. Read only after construction.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = Cluster.Builder.class)
public class Cluster extends ConfigDTO {

    @NotNull(message = "No cluster status entered and is required.")
    private ClusterStatus status;

    @Size(min = 1, max = 255)
    private String clusterType;

    /**
     * Constructor used only by the build() method of the builder.
     *
     * @param builder The builder to get data from
     */
    protected Cluster(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.clusterType = builder.bClusterType;
    }

    /**
     * Get the cluster status.
     *
     * @return The status of the cluster
     */
    public ClusterStatus getStatus() {
        return this.status;
    }

    /**
     * Get the type of the cluster.
     *
     * @return The type of the cluster
     */
    //TODO: Remove cluster type if we don't need it
    public String getClusterType() {
        return this.clusterType;
    }

    /**
     * A builder to create clusters.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ConfigDTO.Builder<Builder> {

        private final ClusterStatus bStatus;
        private final String bClusterType;

        /**
         * Constructor which has required fields.
         *
         * @param name        The name to use for the Cluster
         * @param user        The user to use for the Cluster
         * @param version     The version to use for the Cluster
         * @param status      The status of the Cluster
         * @param clusterType The type of the Cluster [yarn, presto, etc]
         */
        public Builder(
            @JsonProperty("name")
            final String name,
            @JsonProperty("user")
            final String user,
            @JsonProperty("version")
            final String version,
            @JsonProperty("status")
            final ClusterStatus status,
            @JsonProperty("clusterType")
            final String clusterType
        ) {
            super(name, user, version);
            this.bStatus = status;
            this.bClusterType = clusterType;
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
