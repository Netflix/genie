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
package com.netflix.genie.core.jpa.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.Size;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The common entity fields for all Genie entities.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@MappedSuperclass
public class CommonFieldsEntity extends BaseEntity {
    protected static final String GENIE_TAG_NAMESPACE = "genie.";
    protected static final String GENIE_ID_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "id:";
    protected static final String GENIE_NAME_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "name:";
    protected static final String PIPE = "|";
    protected static final String PIPE_REGEX = "\\" + PIPE;

    private static final long serialVersionUID = -5040659007494311180L;

    @Basic(optional = false)
    @Column(name = "version", nullable = false)
    @NotBlank(message = "Version is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String version;

    @Basic(optional = false)
    @Column(name = "user", nullable = false)
    @NotBlank(message = "User name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String user;

    @Basic(optional = false)
    @Column(name = "name", nullable = false)
    @NotBlank(message = "Name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String name;

    @Basic
    @Column(name = "description", length = 5000)
    @Size(max = 5000, message = "Max length in database is 5000 characters")
    private String description;

    @Basic
    @Column(name = "tags", length = 2048)
    @Size(max = 2048, message = "Max length in database is 2048 characters")
    private String tags;

    /**
     * Default constructor.
     */
    public CommonFieldsEntity() {
        super();
    }

    /**
     * Gets the version of this entity.
     *
     * @return version
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Sets the version for this entity.
     *
     * @param version version number for this entity
     */
    public void setVersion(final String version) {
        this.version = version;
    }

    /**
     * Gets the user that created this entity.
     *
     * @return user
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the user who created this entity.
     *
     * @param user user who created this entity. Not null/empty/blank.
     */
    public void setUser(final String user) {
        this.user = user;
    }


    /**
     * Gets the name for this entity.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this entity.
     *
     * @param name the new name of this entity. Not null/empty/blank
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Gets the description of this entity.
     *
     * @return description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Sets the description for this entity.
     *
     * @param description description for the entity. Not null/empty/blank
     */
    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * Get the tags attached to this entity.
     *
     * @return The tags attached to this entity
     */
    public Set<String> getTags() {
        final Set<String> returnTags = new HashSet<>();

        if (this.tags != null) {
            returnTags.addAll(Arrays.asList(this.tags.split(PIPE_REGEX)));
        }

        return returnTags;
    }

    /**
     * Set the tags.
     *
     * @param tags The tags to set
     */
    public void setTags(final Set<String> tags) {
        this.tags = null;
        if (tags != null && !tags.isEmpty()) {
            this.tags = tags
                .stream()
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .reduce((one, two) -> one + PIPE + two)
                .get();
        }
    }

    /**
     * Get the tags with the current genie.id and genie.name tags added into the set.
     *
     * @return The final set of tags for storing in the database
     * @throws GenieException On any exception
     */
    protected Set<String> getFinalTags() throws GenieException {
        final Set<String> finalTags;
        if (this.tags == null) {
            finalTags = Sets.newHashSet();
        } else {
            finalTags = Sets.newHashSet(this.tags.split(PIPE_REGEX))
                .stream()
                .filter(tag -> !tag.contains(GENIE_TAG_NAMESPACE))
                .collect(Collectors.toSet());
        }
        if (this.getId() == null) {
            this.setId(UUID.randomUUID().toString());
        }
        finalTags.add(GENIE_ID_TAG_NAMESPACE + this.getId());
        finalTags.add(GENIE_NAME_TAG_NAMESPACE + this.getName());
        return finalTags;
    }
}
