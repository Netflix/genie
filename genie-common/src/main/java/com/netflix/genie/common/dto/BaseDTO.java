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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.genie.common.util.JsonDateSerializer;

import javax.validation.constraints.Size;
import java.util.Date;

/**
 * Base fields for multiple DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public abstract class BaseDTO {

    @Size(max = 255, message = "Max length is 255 characters")
    private String id;

    @JsonSerialize(using = JsonDateSerializer.class)
    private Date created;

    @JsonSerialize(using = JsonDateSerializer.class)
    private Date updated;

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    protected BaseDTO(final Builder builder) {
        this.id = builder.bId;
        if (builder.bCreated != null) {
            this.created = new Date(builder.bCreated.getTime());
        }
        if (builder.bUpdated != null) {
            this.updated = new Date(builder.bUpdated.getTime());
        }
    }

    /**
     * Get the id.
     *
     * @return The id
     */
    public String getId() {
        return this.id;
    }

    /**
     * Get the creation time.
     *
     * @return The creation time or null if not set.
     */
    public Date getCreated() {
        if (this.created != null) {
            return new Date(this.created.getTime());
        } else {
            return null;
        }
    }

    /**
     * Get the update time.
     *
     * @return The update time or null if not set.
     */
    public Date getUpdated() {
        if (this.updated != null) {
            return new Date(this.updated.getTime());
        } else {
            return null;
        }
    }

    /**
     * Convert this object to a string representation.
     *
     * @return This application data represented as a JSON structure
     */
    @Override
    public String toString() {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
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
    protected abstract static class Builder<T extends Builder> {

        private String bId;
        private Date bCreated;
        private Date bUpdated;

        protected Builder() {
        }

        /**
         * Set the id for the resource.
         *
         * @param id The id
         * @return The builder
         */
        public T withId(final String id) {
            this.bId = id;
            return (T) this;
        }

        /**
         * Set the created time for the resource.
         *
         * @param created The created time
         * @return The builder
         */
        public T withCreated(final Date created) {
            if (created != null) {
                this.bCreated = new Date(created.getTime());
            }
            return (T) this;
        }

        /**
         * Set the updated time for the resource.
         *
         * @param updated The updated time
         * @return The builder
         */
        public T withUpdated(final Date updated) {
            if (updated != null) {
                this.bUpdated = new Date(updated.getTime());
            }
            return (T) this;
        }
    }
}
