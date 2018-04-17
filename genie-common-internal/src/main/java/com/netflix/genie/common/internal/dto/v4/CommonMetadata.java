/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.common.internal.dto.v4;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.util.GenieObjectMapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metadata fields common to all Genie resources (Jobs, clusters, etc).
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public abstract class CommonMetadata {
    @NotBlank(message = "A name is required and must be at most 255 characters")
    @Size(max = 255, message = "The name can be no longer than 255 characters")
    private final String name;
    @NotBlank(message = "A user is required and must be at most 255 characters")
    @Size(max = 255, message = "The user can be no longer than 255 characters")
    private final String user;
    @NotBlank(message = "A version is required and must be at most 255 characters")
    @Size(max = 255, message = "The version can be no longer than 255 characters")
    private final String version;
    @Size(max = 1000, message = "The description can be no longer than 1000 characters")
    private final String description;
    private final JsonNode metadata;
    private final ImmutableSet<
        @NotEmpty(message = "A tag can't be an empty string")
        @Size(max = 255, message = "A tag can't be longer than 255 characters") String> tags;

    /**
     * Constructor.
     *
     * @param builder The builder containing the values to use.
     */
    @SuppressWarnings("unchecked")
    CommonMetadata(final Builder builder) {
        this.name = builder.bName;
        this.user = builder.bUser;
        this.version = builder.bVersion;
        this.description = builder.bDescription;
        this.metadata = builder.bMetadata;
        this.tags = builder.bTags == null ? ImmutableSet.of() : ImmutableSet.copyOf(builder.bTags);
    }

    /**
     * Get the description.
     *
     * @return The description as an {@link Optional}
     */
    public Optional<String> getDescription() {
        return Optional.ofNullable(this.description);
    }

    /**
     * Get the metadata of this resource as a JSON Node.
     *
     * @return {@link Optional} of the metadata if it exists
     */
    public Optional<JsonNode> getMetadata() {
        return Optional.ofNullable(this.metadata);
    }

    /**
     * Get the tags associated with this resource. Will be returned as an immutable set and any attempt to modify will
     * result in an exception being thrown.
     *
     * @return The tags
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Builder for common fields.
     *
     * @param <T> Type of builder that extends this
     * @author tgianos
     * @since 4.0.0
     */
    // NOTE: These abstract class builders are marked public not protected due to a JDK bug from 1999 which caused
    //       issues with Clojure clients which use reflection to make the Java API calls.
    //       http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4283544
    //       Setting them to public seems to have solved the issue at the expense of "proper" code design
    @SuppressWarnings("unchecked")
    public static class Builder<T extends Builder> {

        private final String bName;
        private final String bUser;
        private final String bVersion;
        private String bDescription;
        private JsonNode bMetadata;
        private ImmutableSet<String> bTags;

        /**
         * Constructor with required fields.
         *
         * @param name    The name of the resource
         * @param user    The user owning the resource
         * @param version The version of hte resource
         */
        protected Builder(
            final String name,
            final String user,
            final String version
        ) {
            this.bName = name;
            this.bUser = user;
            this.bVersion = version;
        }

        /**
         * Set the description for the resource.
         *
         * @param description The description to use
         * @return The builder
         */
        public T withDescription(@Nullable final String description) {
            this.bDescription = StringUtils.isBlank(description) ? null : description;
            return (T) this;
        }

        /**
         * Set the tags to use for the resource.
         *
         * @param tags The tags to use. Blanks will be removed
         * @return The builder
         */
        public T withTags(@Nullable final Set<String> tags) {
            this.bTags = tags == null ? ImmutableSet.of() : ImmutableSet.copyOf(
                tags
                    .stream()
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toSet())
            );
            return (T) this;
        }

        /**
         * With the metadata to set for the job as a JsonNode.
         *
         * @param metadata The metadata to set
         * @return The builder
         */
        @JsonSetter
        public T withMetadata(@Nullable final JsonNode metadata) {
            this.bMetadata = metadata;
            return (T) this;
        }

        /**
         * With the ad-hoc metadata to set for the resource as a string of valid JSON.
         *
         * @param metadata The metadata to set. Must be valid JSON
         * @return The builder
         * @throws GeniePreconditionException On invalid JSON
         */
        public T withMetadata(@Nullable final String metadata) throws GeniePreconditionException {
            if (metadata == null) {
                this.bMetadata = null;
            } else {
                try {
                    this.bMetadata = GenieObjectMapper.getMapper().readTree(metadata);
                } catch (final IOException ioe) {
                    throw new GeniePreconditionException("Invalid metadata JSON string passed in " + metadata, ioe);
                }
            }
            return (T) this;
        }
    }
}
