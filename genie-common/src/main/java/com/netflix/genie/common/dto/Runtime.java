/*
 *
 *  Copyright 2022 Netflix, Inc.
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
package com.netflix.genie.common.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * DTO for metadata related to the runtime environment of a given job.
 *
 * @author tgianos
 * @since 4.3.0
 */
@JsonDeserialize(builder = Runtime.Builder.class)
public class Runtime implements Serializable {
    private final RuntimeResources resources;
    private final Map<String, ContainerImage> images;

    private Runtime(final Builder builder) {
        this.resources = builder.bResources;
        this.images = Collections.unmodifiableMap(new HashMap<>(builder.bImages));
    }

    /**
     * Get the compute resources for this runtime.
     *
     * @return The resources
     */
    public RuntimeResources getResources() {
        return this.resources;
    }

    /**
     * The container images defined.
     *
     * @return The images that were defined as an immutable map. Any attempt to modify will throw exception.
     */
    public Map<String, ContainerImage> getImages() {
        return this.images;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Runtime)) {
            return false;
        }
        final Runtime runtime = (Runtime) o;
        return this.resources.equals(runtime.resources) && this.images.equals(runtime.images);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.resources, this.images);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Runtime{"
            + "resources=" + this.resources
            + ", images=" + this.images
            + '}';
    }

    /**
     * Builder class for {@link Runtime} instances.
     */
    public static class Builder {
        private RuntimeResources bResources;
        private final Map<String, ContainerImage> bImages;

        /**
         * Constructor.
         */
        public Builder() {
            this.bResources = new RuntimeResources.Builder().build();
            this.bImages = new HashMap<>();
        }

        /**
         * Set the compute runtime resources to use.
         *
         * @param resources The {@link RuntimeResources} to use
         * @return This {@link Builder} instance
         */
        public Builder withResources(@Nullable final RuntimeResources resources) {
            if (resources == null) {
                this.bResources = new RuntimeResources.Builder().build();
            } else {
                this.bResources = resources;
            }
            return this;
        }

        /**
         * Set any container images needed with this resource (job, command, etc).
         *
         * @param images The map of system-wide image key to {@link ContainerImage} definition.
         * @return This {@link Builder} instance
         */
        public Builder withImages(@Nullable final Map<String, ContainerImage> images) {
            this.bImages.clear();
            if (images != null) {
                this.bImages.putAll(images);
            }
            return this;
        }

        /**
         * Create a new immutable {@link Runtime} instance based on the current contents of this builder instance.
         *
         * @return A new {@link Runtime} instance
         */
        public Runtime build() {
            return new Runtime(this);
        }
    }
}
