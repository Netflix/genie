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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.time.Instant;

/**
 * An immutable V4 Cluster resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true, doNotUseGetters = true)
public class Cluster extends CommonResource {
    @Valid
    private final ClusterMetadata metadata;

    /**
     * Constructor.
     *
     * @param id        The unique identifier of this cluster
     * @param created   The time this cluster was created in the system
     * @param updated   The last time this cluster was updated in the system
     * @param resources The execution resources associated with this cluster
     * @param metadata  The metadata associated with this cluster
     */
    @JsonCreator
    public Cluster(
        @JsonProperty(value = "id", required = true) final String id,
        @JsonProperty(value = "created", required = true) final Instant created,
        @JsonProperty(value = "updated", required = true) final Instant updated,
        @JsonProperty(value = "resources") @Nullable final ExecutionEnvironment resources,
        @JsonProperty(value = "metadata", required = true) final ClusterMetadata metadata
    ) {
        super(id, created, updated, resources);
        this.metadata = metadata;
    }
}
