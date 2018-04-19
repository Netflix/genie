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

import javax.validation.Valid;

/**
 * Fields representing all the values users can set when creating a new Cluster resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = ClusterRequest.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class ClusterRequest extends CommonRequestImpl {

    @Valid
    private final ClusterMetadata metadata;

    private ClusterRequest(final Builder builder) {
        super(builder);
        this.metadata = builder.bMetadata;
    }

    /**
     * Builder for a V4 Cluster Request.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonRequestImpl.Builder<Builder> {

        private final ClusterMetadata bMetadata;

        /**
         * Constructor which has required fields.
         *
         * @param metadata The user supplied metadata about a cluster resource
         */
        @JsonCreator
        public Builder(@JsonProperty(value = "metadata", required = true) final ClusterMetadata metadata) {
            super();
            this.bMetadata = metadata;
        }

        /**
         * Build a new ClusterRequest instance.
         *
         * @return The immutable cluster request
         */
        public ClusterRequest build() {
            return new ClusterRequest(this);
        }
    }
}
