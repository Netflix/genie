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
package com.netflix.genie.web.selectors.impl;

import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.selectors.ResourceSelector;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

/**
 * A base class for any selector which desires to select a random element from a collection of elements.
 *
 * @param <R> The type of resource the collection has
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
class RandomResourceSelector<R> implements ResourceSelector<R> {

    static final String SELECTION_RATIONALE = "Selected randomly";
    private final Random random = new Random();

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectionResult<R> select(
        @NotEmpty final Set<@Valid R> resources,
        @Valid final JobRequest jobRequest,
        @NotBlank final String jobId
    ) throws ResourceSelectionException {
        log.debug("Called to select for job {}", jobId);
        final ResourceSelectionResult.Builder<R> builder = new ResourceSelectionResult.Builder<>(this.getClass());

        try {
            final R selectedResource = this.randomlySelect(resources);
            return builder.withSelectionRationale(SELECTION_RATIONALE).withSelectedResource(selectedResource).build();
        } catch (final Exception e) {
            throw new ResourceSelectionException(e);
        }
    }

    @Nullable
    private R randomlySelect(@NotEmpty final Collection<R> resources) {
        // return a random one
        final int index = this.random.nextInt(resources.size());
        final Iterator<R> resourceIterator = resources.iterator();
        R selectedResource = null;
        int i = 0;
        while (resourceIterator.hasNext() && i <= index) {
            selectedResource = resourceIterator.next();
            i++;
        }
        return selectedResource;
    }
}
