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
import com.netflix.genie.common.dto.ClusterStatus;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.constraints.NotNull;

/**
 * Metadata supplied by a user for a Cluster resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = ClusterMetadata.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class ClusterMetadata extends CommonMetadata {

    @NotNull(message = "A cluster status is required")
    private final ClusterStatus status;

    private ClusterMetadata(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
    }


    /**
     * A builder to create cluster user metadata instances.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonMetadata.Builder<Builder> {

        private final ClusterStatus bStatus;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the cluster
         * @param user    The user who owns the cluster
         * @param version The version of the cluster
         * @param status  The status of the cluster
         */
        @JsonCreator
        public Builder(
            @JsonProperty(value = "name", required = true) final String name,
            @JsonProperty(value = "user", required = true) final String user,
            @JsonProperty(value = "version", required = true) final String version,
            @JsonProperty(value = "status", required = true) final ClusterStatus status
        ) {
            super(name, user, version);
            this.bStatus = status;
        }

        /**
         * Build the cluster metadata instance.
         *
         * @return Create the final read-only clusterMetadata instance
         */
        public ClusterMetadata build() {
            return new ClusterMetadata(this);
        }
    }
}
