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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.genie.common.util.JsonDateSerializer;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Base fields for multiple DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public abstract class BaseDTO {

    @ApiModelProperty(
            value = "The unique id of this resource. If one is not provided it is created internally"
    )
    @Length(max = 255, message = "Max length is 255 characters")
    private String id;

    @ApiModelProperty(
            value = "When this resource was created. Set automatically by system",
            readOnly = true,
            dataType = "dateTime"
    )
    @JsonSerialize(using = JsonDateSerializer.class)
    private Date created;

    @ApiModelProperty(
            value = "When this resource was last updated. Set automatically by system",
            readOnly = true,
            dataType = "dateTime"
    )
    @JsonSerialize(using = JsonDateSerializer.class)
    private Date updated;

    @ApiModelProperty(
            value = "The version number",
            required = true
    )
    @NotBlank(message = "Version is missing and is required.")
    @Length(max = 255, message = "Max length is 255 characters")
    private String version;

    @ApiModelProperty(
            value = "User who created/owns this object",
            required = true
    )
    @NotBlank(message = "User name is missing and is required.")
    @Length(max = 255, message = "Max length is 255 characters")
    private String user;

    @ApiModelProperty(
            value = "The name to use",
            required = true
    )
    @NotBlank(message = "Name is missing and is required.")
    @Length(max = 255, message = "Max length is 255 characters")
    private String name;

    @ApiModelProperty(
            value = "The description of this entity",
            required = true
    )
    private String description;

    @ApiModelProperty(
            value = "The tags associated with this entity",
            required = true
    )
    private final Set<String> tags = new HashSet<>();

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
        this.name = builder.bName;
        this.user = builder.bUser;
        this.version = builder.bVersion;
        this.description = builder.bDescription;
        if (builder.bTags != null) {
            this.tags.addAll(builder.bTags);
        }
    }

    /**
     * Constructor.
     *
     * @param id          The id for the dto
     * @param created     The creation time
     * @param updated     The update time
     * @param name        The name
     * @param user        The user
     * @param version     The version
     * @param description The description
     * @param tags        The tags
     */
    protected BaseDTO(
            final String id,
            final Date created,
            final Date updated,
            final String name,
            final String user,
            final String version,
            final String description,
            final Set<String> tags
    ) {
        this.id = id;
        if (created != null) {
            this.created = new Date(created.getTime());
        }
        if (updated != null) {
            this.updated = new Date(updated.getTime());
        }
        this.name = name;
        this.user = user;
        this.version = version;
        this.description = description;
        if (tags != null) {
            this.tags.addAll(tags);
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
     * Get the version.
     *
     * @return the version
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Get the user.
     *
     * @return The user
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Get the name.
     *
     * @return The name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the description.
     *
     * @return The desciption
     */
    public String getDescription() {
        return this.description;
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
    protected abstract static class Builder<T extends Builder> {

        private String bId;
        private Date bCreated;
        private Date bUpdated;
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
