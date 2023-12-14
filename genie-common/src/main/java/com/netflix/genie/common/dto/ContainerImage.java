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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation of metadata corresponding to the container image (docker, etc.) that the job should be launched in.
 *
 * @author tgianos
 * @since 4.3.0
 */
@JsonDeserialize(builder = ContainerImage.Builder.class)
public class ContainerImage implements Serializable {

    private final String name;
    private final String tag;
    private final List<String> arguments;

    private ContainerImage(final Builder builder) {
        this.name = builder.bName;
        this.tag = builder.bTag;
        this.arguments = Collections.unmodifiableList(new ArrayList<>(builder.bArguments));
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
     * Get the image arguments if any.
     *
     * @return An unmodifiable list of arguments. Any attempt to modify will throw an exception
     */
    public List<String> getArguments() {
        return this.arguments;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.name, this.tag, this.arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContainerImage)) {
            return false;
        }
        final ContainerImage containerImage = (ContainerImage) o;
        return Objects.equals(this.name, containerImage.name)
            && Objects.equals(this.tag, containerImage.tag)
            && Objects.equals(this.arguments, containerImage.arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "ContainerImage{"
            + "name='" + this.name + '\''
            + ", tag='" + this.tag + '\''
            + ", arguments='" + this.arguments + '\''
            + '}';
    }

    /**
     * Builder for immutable instances of {@link ContainerImage}.
     *
     * @author tgianos
     * @since 4.3.0
     */
    public static class Builder {
        private String bName;
        private String bTag;
        private final List<String> bArguments;

        /**
         * Constructor.
         */
        public Builder() {
            this.bArguments = new ArrayList<>();
        }


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
         * Set the arguments for the image.
         *
         * @param arguments The arguments. {@literal null} will clear any currently set arguments, as will empty list.
         *                  Any other value with replace.
         * @return This {@link Builder} instance
         */
        public Builder withArguments(@Nullable final List<String> arguments) {
            this.bArguments.clear();
            if (arguments != null) {
                this.bArguments.addAll(arguments);
            }
            return this;
        }

        /**
         * Create an immutable instance of {@link ContainerImage} based on the current contents of this builder
         * instance.
         *
         * @return A new {@link ContainerImage} instance
         */
        public ContainerImage build() {
            return new ContainerImage(this);
        }
    }
}
