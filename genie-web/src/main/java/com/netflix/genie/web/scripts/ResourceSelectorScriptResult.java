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
package com.netflix.genie.web.scripts;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Class to represent a generic response from a script which selects a resource from a set of resources.
 *
 * @param <R> The type of resource that was selected
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@JsonDeserialize(builder = ResourceSelectorScriptResult.Builder.class)
@SuppressWarnings("FinalClass")
public class ResourceSelectorScriptResult<R> {
    private final R resource;
    private final String rationale;

    private ResourceSelectorScriptResult(final Builder<R> builder) {
        this.resource = builder.bResource;
        this.rationale = builder.bRationale;
    }

    /**
     * Get the selected resource if there was one.
     *
     * @return The resource wrapped in an {@link Optional} or {@link Optional#empty()}
     */
    public Optional<R> getResource() {
        return Optional.ofNullable(this.resource);
    }

    /**
     * Get the rationale for the selection decision.
     *
     * @return The rationale wrapped in an {@link Optional} or {@link Optional#empty()}
     */
    public Optional<String> getRationale() {
        return Optional.ofNullable(this.rationale);
    }

    /**
     * A builder for these the results to prevent scripts from having to redo everything based on constructors if
     * we change parameters.
     *
     * @param <R> The type of resource that was selected
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder<R> {
        private R bResource;
        private String bRationale;

        /**
         * Set the resource that was selected if any.
         *
         * @param resource The resource that was selected or {@literal null}
         * @return The builder instance
         */
        public Builder<R> withResource(@Nullable final R resource) {
            this.bResource = resource;
            return this;
        }

        /**
         * Set the rationale for the selection or lack thereof.
         *
         * @param rationale The rationale or {@literal null} if there was none
         * @return The builder instance
         */
        public Builder<R> withRationale(@Nullable final String rationale) {
            this.bRationale = rationale;
            return this;
        }

        /**
         * Build a new instance of a {@link ResourceSelectorScriptResult} based on the contents of this builder.
         *
         * @return A new instance of {@link ResourceSelectorScriptResult}
         */
        public ResourceSelectorScriptResult<R> build() {
            return new ResourceSelectorScriptResult<>(this);
        }
    }
}
