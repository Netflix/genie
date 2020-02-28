/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.common.external.dtos.v4;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Set;

/**
 * Representing the result of resolving resources of type {@literal R} from a {@link Criterion}.
 *
 * @param <R> The type of the resource that was resolved
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public class ResolvedResources<R> {

    private final Criterion criterion;
    private final ImmutableSet<R> resources;

    /**
     * Constructor.
     *
     * @param criterion The {@link Criterion} that was used to resolve {@literal resources}
     * @param resources The resources that were resolved based on the {@literal criterion}
     */
    @JsonCreator
    public ResolvedResources(
        @JsonProperty(value = "criterion", required = true) final Criterion criterion,
        @JsonProperty(value = "resources", required = true) final Set<R> resources
    ) {
        this.criterion = criterion;
        this.resources = ImmutableSet.copyOf(resources);
    }

    /**
     * Get the resources that were resolved.
     *
     * @return The resolved resources as an immutable {@link Set}. Any attempt to modify will cause error.
     */
    public Set<R> getResources() {
        return this.resources;
    }
}
