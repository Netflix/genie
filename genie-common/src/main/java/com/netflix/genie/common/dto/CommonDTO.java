/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import lombok.Getter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Common fields for multiple DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public abstract class CommonDTO extends BaseDTO {

    private static final long serialVersionUID = -2082573569004634251L;

    private final Set<String> tags = new HashSet<>();

    @NotEmpty(message = "A version is required and must be at most 255 characters.")
    @Size(max = 255, message = "The version can be no longer than 255 characters")
    private final String version;
    @NotEmpty(message = "A user is required and must be at most 255 characters")
    @Size(max = 255, message = "The user can be no longer than 255 characters")
    private final String user;
    @NotEmpty(message = "A name is required and must be at most 255 characters")
    @Size(max = 255, message = "The name can be no longer than 255 characters")
    private final String name;
    @Size(max = 10000, message = "The maximum length of the description field is 10000 characaters")
    private final String description;

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    @SuppressWarnings("unchecked")
    protected CommonDTO(final Builder builder) {
        super(builder);
        this.name = builder.bName;
        this.user = builder.bUser;
        this.version = builder.bVersion;
        this.description = builder.bDescription;
        this.tags.addAll(builder.bTags);
    }

    /**
     * Get the description.
     *
     * @return The description as an optional
     */
    public Optional<String> getDescription() {
        return Optional.ofNullable(this.description);
    }

    /**
     * Get a readonly copy of the tags.
     *
     * @return The tags. Read only. Will throw exception if try to modify.
     */
    public Set<String> getTags() {
        return Collections.unmodifiableSet(this.tags);
    }

    /**
     * Builder pattern to save constructor arguments.
     *
     * @param <T> Type of builder that extends this
     * @author tgianos
     * @since 3.0.0
     */
    @SuppressWarnings("unchecked")
    protected abstract static class Builder<T extends Builder> extends BaseDTO.Builder<T> {

        private final String bName;
        private final String bUser;
        private final String bVersion;
        private String bDescription;
        private Set<String> bTags = new HashSet<>();

        protected Builder(final String name, final String user, final String version) {
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
        public T withDescription(final String description) {
            this.bDescription = description;
            return (T) this;
        }

        /**
         * Set the tags to use for the resource.
         *
         * @param tags The tags to use
         * @return The builder
         */
        public T withTags(final Set<String> tags) {
            if (tags != null) {
                this.bTags.addAll(tags);
            }
            return (T) this;
        }
    }
}
