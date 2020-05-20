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

import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.selectors.ResourceSelectionContext;
import com.netflix.genie.web.selectors.ResourceSelector;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

/**
 * A base class for any selector which desires to select a random element from a collection of elements.
 *
 * @param <R> The type of resource the collection has
 * @param <C> The type of context which this selector will accept. Must extend {@link ResourceSelectionContext}
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
class RandomResourceSelector<R, C extends ResourceSelectionContext<R>> implements ResourceSelector<R, C> {

    static final String SELECTION_RATIONALE = "Selected randomly";
    private final Random random = new Random();

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectionResult<R> select(@Valid final C context) throws ResourceSelectionException {
        log.debug("Called to select for job {}", context.getJobId());
        final ResourceSelectionResult.Builder<R> builder = new ResourceSelectionResult.Builder<>(this.getClass());

        try {
            final R selectedResource = this.randomlySelect(context.getResources());
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
