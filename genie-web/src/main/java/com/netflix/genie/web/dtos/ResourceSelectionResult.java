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
package com.netflix.genie.web.dtos;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * A data class for returning the results of an attempted resource selection.
 *
 * @param <R> The resource type this selection result is for
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@SuppressWarnings("FinalClass")
public class ResourceSelectionResult<R> {
    private final Class<?> selectorClass;
    private final R selectedResource;
    private final String selectionRationale;

    private ResourceSelectionResult(final Builder<R> builder) {
        this.selectorClass = builder.bSelectorClass;
        this.selectedResource = builder.bSelectedResource;
        this.selectionRationale = builder.bSelectionRationale;
    }

    /**
     * Get the selected resource if there was one.
     *
     * @return The selected resource wrapped in {@link Optional} else {@link Optional#empty()}
     */
    public Optional<R> getSelectedResource() {
        return Optional.ofNullable(this.selectedResource);
    }

    /**
     * Return any rationale as to why this resource was selected or why no resource was selected if that was the case.
     *
     * @return Any provided rationale or {@link Optional#empty()}
     */
    public Optional<String> getSelectionRationale() {
        return Optional.ofNullable(this.selectionRationale);
    }

    /**
     * A builder for {@link ResourceSelectionResult} instances.
     *
     * @param <R> The type of the selected resource
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder<R> {
        private final Class<?> bSelectorClass;
        private R bSelectedResource;
        private String bSelectionRationale;

        /**
         * Constructor.
         *
         * @param selectorClass The class that generated this result
         */
        public Builder(final Class<?> selectorClass) {
            this.bSelectorClass = selectorClass;
        }

        /**
         * Set the resource that was selected by this selector if any.
         *
         * @param selectedResource The selected resource or {@literal null}
         * @return the builder instance
         */
        public Builder<R> withSelectedResource(@Nullable final R selectedResource) {
            this.bSelectedResource = selectedResource;
            return this;
        }

        /**
         * Set the rationale for why a resource as or wasn't selected.
         *
         * @param selectionRationale The rational or {@literal null}
         * @return the builder instance
         */
        public Builder<R> withSelectionRationale(@Nullable final String selectionRationale) {
            this.bSelectionRationale = selectionRationale;
            return this;
        }

        /**
         * Build a new immutable {@link ResourceSelectionResult} instance out of the current state of this builder.
         *
         * @return A new {@link ResourceSelectionResult} instance that is immutable
         */
        public ResourceSelectionResult<R> build() {
            return new ResourceSelectionResult<>(this);
        }
    }
}
