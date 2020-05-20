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

import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * Generic interface for a selector which selects a resource from a set of resources for a given job request.
 *
 * @param <R> The type of resource this selector is selecting
 * @param <C> The type of context which this selector will accept. Must extend {@link ResourceSelectionContext}
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface ResourceSelector<R, C extends ResourceSelectionContext<R>> {

    /**
     * Select a resource from the given set of resources if possible.
     *
     * @param context The context specific for this resource selection
     * @return The a {@link ResourceSelectionResult} instance which contains information about the result of this
     * invocation
     * @throws ResourceSelectionException When the underlying implementation can't successfully come to a selection
     *                                    decision
     */
    ResourceSelectionResult<R> select(@Valid C context) throws ResourceSelectionException;
}
