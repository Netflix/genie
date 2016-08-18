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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.netflix.genie.common.util.GenieDateFormat;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import javax.validation.constraints.Size;
import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

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
    private static final ObjectMapper MAPPER;

    static {
        final DateFormat iso8601 = new GenieDateFormat();
        iso8601.setTimeZone(TimeZone.getTimeZone("UTC"));
        MAPPER = new ObjectMapper().registerModule(new Jdk8Module()).setDateFormat(iso8601);
    }

    @Size(max = 255, message = "Max length for the ID is 255 characters")
    private final String id;
    private final Date created;
    private final Date updated;

    /**
     * Constructor.
     *
     * @param builder The builder to use
     */
    protected BaseDTO(final Builder builder) {
        this.id = builder.bId;
        this.created = builder.bCreated == null ? null : new Date(builder.bCreated.getTime());
        this.updated = builder.bUpdated == null ? null : new Date(builder.bUpdated.getTime());
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
    public Optional<Date> getCreated() {
        if (this.created != null) {
            return Optional.of(new Date(this.created.getTime()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Get the update time.
     *
     * @return The update time or null if not set.
     */
    public Optional<Date> getUpdated() {
        if (this.updated != null) {
            return Optional.of(new Date(this.updated.getTime()));
        } else {
            return Optional.empty();
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
            return MAPPER.writeValueAsString(this);
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
    @SuppressWarnings("unchecked")
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
