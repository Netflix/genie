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
package com.netflix.genie.common.internal.dtos;

import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation of metadata corresponding to the container image (docker, etc.) that the job should be launched in.
 *
 * @author tgianos
 * @since 4.3.0
 */
public class Image {

    @Size(max = 1024, message = "Maximum length of a container image name is 1024 characters")
    private final String name;
    @Size(max = 1024, message = "Maximum length of a container image tag is 1024 characters")
    private final String tag;

    private Image(final Builder builder) {
        this.name = builder.bName;
        this.tag = builder.bTag;
    }

    /**
     * Get the name of the image to use for the job if one was specified.
     *
     * @return The name or {@link Optional#empty()}
     */
    public Optional<String> getName() {
        return Optional.ofNullable(this.name);
    }

    /**
     * Get the tag of the image to use for the job if one was specified.
     *
     * @return The tag or {@link Optional#empty()}
     */
    public Optional<String> getTag() {
        return Optional.ofNullable(this.tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Image)) {
            return false;
        }
        final Image image = (Image) o;
        return Objects.equals(this.name, image.name) && Objects.equals(this.tag, image.tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "Image{"
            + "name='" + name + '\''
            + ", tag='" + tag + '\''
            + '}';
    }

    /**
     * Builder for immutable instances of {@link Image}.
     *
     * @author tgianos
     * @since 4.3.0
     */
    public static class Builder {
        private String bName;
        private String bTag;


        /**
         * Set the name of the image to use.
         *
         * @param name The name or {@literal null}
         * @return This {@link Builder} instance
         */
        public Builder withName(@Nullable final String name) {
            this.bName = name;
            return this;
        }

        /**
         * Set the tag of the image to use.
         *
         * @param tag The tag or {@literal null}
         * @return This {@link Builder} instance
         */
        public Builder withTag(@Nullable final String tag) {
            this.bTag = tag;
            return this;
        }

        /**
         * Create an immutable instance of {@link Image} based on the current contents of this builder instance.
         *
         * @return A new {@link Image} instance
         */
        public Image build() {
            return new Image(this);
        }
    }
}
