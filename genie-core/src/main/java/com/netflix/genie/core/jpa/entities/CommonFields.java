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
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Lob;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.Size;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The common entity fields for all Genie entities.
 *
 * @author amsharma
 * @author tgianos
 */
@MappedSuperclass
public class CommonFields extends BaseEntity {
    protected static final String GENIE_TAG_NAMESPACE = "genie.";
    protected static final String GENIE_ID_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "id:";
    protected static final String GENIE_NAME_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "name:";
    protected static final String COMMA = ",";

    @Basic(optional = false)
    @Column(name = "version", nullable = false, length = 255)
    @NotBlank(message = "Version is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String version;

    @Basic(optional = false)
    @Column(name = "user", nullable = false, length = 255)
    @NotBlank(message = "User name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String user;

    @Basic(optional = false)
    @Column(name = "name", nullable = false, length = 255)
    @NotBlank(message = "Name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String name;

    @Lob
    @Basic
    @Column(name = "description")
    private String description;

    @Basic
    @Column(name = "original_tags", length = 2048)
    @Size(max = 2048, message = "Max length in database is 2048 characters")
    private String originalTags;

    @Basic
    @Column(name = "sorted_tags", length = 2048)
    @Size(max = 2048, message = "Max length in database is 2048 characters")
    private String sortedTags;

    /**
     * Default constructor.
     */
    public CommonFields() {
        super();
    }

    /**
     * Construct a new CommonEntity Object with all required parameters.
     *
     * @param name    The name of the entity. Not null/empty/blank.
     * @param user    The user who created the entity. Not null/empty/blank.
     * @param version The version of this entity. Not null/empty/blank.
     */
    public CommonFields(
            final String name,
            final String user,
            final String version) {
        super();
        this.name = name;
        this.user = user;
        this.version = version;
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
     * Get the original tags as csv.
     *
     * @return The original tags delimited with a comma
     */
    protected String getOriginalTags() {
        return this.originalTags;
    }

    /**
     * Set the original tags csv.
     *
     * @param originalTags the string
     */
    protected void setOriginalTags(final String originalTags) {
        this.originalTags = originalTags;
    }

    /**
     * Get the original tags as a sorted lowercase csv.
     *
     * @return The sorted tags
     */
    protected String getSortedTags() {
        return this.sortedTags;
    }

    /**
     * Set the original tags as a sorted lowercase csv.
     *
     * @param sortedTags The new sorted tags
     */
    protected void setSortedTags(final String sortedTags) {
        this.sortedTags = sortedTags;
    }

    protected void setOriginalAndSortedTags(final Set<String> tags) {
        this.originalTags = null;
        this.sortedTags = null;
        if (tags != null && !tags.isEmpty()) {
            this.originalTags = StringUtils.join(tags, COMMA);
            this.sortedTags = tags
                    .stream()
                    .map(String::toLowerCase)
                    .sorted()
                    .reduce((one, two) -> one + COMMA + two)
                    .get();
        }
    }

    protected Set<String> getFinalTags() throws GenieException {
        final Set<String> finalTags
                = this.getOriginalTags() == null
                ? Sets.newHashSet()
                : Sets.newHashSet(this.getOriginalTags().split(COMMA))
                .stream()
                .filter(tag -> !tag.contains(GENIE_TAG_NAMESPACE))
                .collect(Collectors.toSet());
        if (this.getId() == null) {
            this.setId(UUID.randomUUID().toString());
        }
        finalTags.add(GENIE_ID_TAG_NAMESPACE + this.getId());
        finalTags.add(GENIE_NAME_TAG_NAMESPACE + this.getName());
        return finalTags;
    }
}
