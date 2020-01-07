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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;

/**
 * Base fields for multiple DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@EqualsAndHashCode(of = "id", doNotUseGetters = true)
public abstract class BaseDTO implements Serializable {
    private static final long serialVersionUID = 9093424855934127120L;

    @Size(max = 255, message = "Max length for the ID is 255 characters")
    private final String id;
    private final Instant created;
    private final Instant updated;

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    BaseDTO(final Builder builder) {
        this.id = builder.bId;
        this.created = builder.bCreated;
        this.updated = builder.bUpdated;
    }

    /**
     * Get the Id of this DTO.
     *
     * @return The id as an Optional
     */
    public Optional<String> getId() {
        return Optional.ofNullable(this.id);
    }

    /**
     * Get the creation time.
     *
     * @return The creation time or null if not set.
     */
    public Optional<Instant> getCreated() {
        return Optional.ofNullable(this.created);
    }

    /**
     * Get the update time.
     *
     * @return The update time or null if not set.
     */
    public Optional<Instant> getUpdated() {
        return Optional.ofNullable(this.updated);
    }

    /**
     * Convert this object to a string representation.
     *
     * @return This application data represented as a JSON structure
     */
    @Override
    public String toString() {
        try {
            return GenieObjectMapper.getMapper().writeValueAsString(this);
        } catch (final JsonProcessingException ioe) {
            return ioe.getLocalizedMessage();
        }
    }

    /**
     * Builder pattern to save constructor arguments.
     *
     * @param <T> Type of builder that extends this
     * @author tgianos
     * @since 3.0.0
     */
    // NOTE: These abstract class builders are marked public not protected due to a JDK bug from 1999 which caused
    //       issues with Clojure clients which use reflection to make the Java API calls.
    //       http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4283544
    //       Setting them to public seems to have solved the issue at the expense of "proper" code design
    @SuppressWarnings("unchecked")
    public abstract static class Builder<T extends Builder> {

        private String bId;
        private Instant bCreated;
        private Instant bUpdated;

        protected Builder() {
        }

        /**
         * Set the id for the resource.
         *
         * @param id The id
         * @return The builder
         */
        public T withId(@Nullable final String id) {
            this.bId = id;
            return (T) this;
        }

        /**
         * Set the created time for the resource.
         *
         * @param created The created time
         * @return The builder
         */
        public T withCreated(@Nullable final Instant created) {
            this.bCreated = created;
            return (T) this;
        }

        /**
         * Set the updated time for the resource.
         *
         * @param updated The updated time
         * @return The builder
         */
        public T withUpdated(@Nullable final Instant updated) {
            this.bUpdated = updated;
            return (T) this;
        }
    }
}
