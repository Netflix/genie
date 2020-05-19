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
package com.netflix.genie.web.selectors;

import com.netflix.genie.common.external.dtos.v4.JobRequest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Context object for encapsulating state into the selectors.
 *
 * @param <R> The type of resource this context is meant to help select
 * @author tgianos
 * @since 4.0.0
 */
@RequiredArgsConstructor
@Getter
@ToString(doNotUseGetters = true)
public abstract class ResourceSelectionContext<R> {
    @NotBlank
    private final String jobId;
    @NotNull
    private final JobRequest jobRequest;
    private final boolean apiJob;

    /**
     * Return the {@link Set} of distinct resources that a selector is meant to chose from.
     *
     * @return The resources
     */
    public abstract Set<R> getResources();
}
