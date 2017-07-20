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
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.Lob;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Optional;
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
@Getter
@Setter
@MappedSuperclass
public class CommonFieldsEntity extends BaseEntity {
    /**
     * The delimiter used to separate tags in the database.
     */
    public static final String TAG_DELIMITER = "|";
    protected static final String GENIE_TAG_NAMESPACE = "genie.";
    protected static final String GENIE_ID_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "id:";
    protected static final String GENIE_NAME_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "name:";
    protected static final String TAG_DELIMITER_REGEX = "\\" + TAG_DELIMITER + "\\" + TAG_DELIMITER;

    private static final long serialVersionUID = -5040659007494311180L;

    @Basic(optional = false)
    @Column(name = "version", nullable = false)
    @NotBlank(message = "Version is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String version;

    @Basic(optional = false)
    @Column(name = "genie_user", nullable = false)
    @NotBlank(message = "User name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String user;

    @Basic(optional = false)
    @Column(name = "name", nullable = false)
    @NotBlank(message = "Name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String name;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "description")
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
     * Gets the description of this entity.
     *
     * @return description
     */
    public Optional<String> getDescription() {
        return Optional.ofNullable(this.description);
    }

    /**
     * Get the tags attached to this entity.
     *
     * @return The tags attached to this entity
     */
    public Set<String> getTags() {
        if (this.tags != null) {
            return Sets.newHashSet(this.splitTags(this.tags));
        } else {
            return Sets.newHashSet();
        }
    }

    /**
     * Set the tags.
     *
     * @param tags The tags to set
     */
    public void setTags(final Set<String> tags) {
        this.tags = null;
        if (tags != null && !tags.isEmpty()) {
            this.tags = TAG_DELIMITER
                + tags
                .stream()
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .reduce((one, two) -> one + TAG_DELIMITER + TAG_DELIMITER + two)
                .orElse("")
                + TAG_DELIMITER;
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
            finalTags = Sets.newHashSet(this.splitTags(this.tags))
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

    @NotNull
    private String[] splitTags(@NotNull final String tagsToSplit) {
        return tagsToSplit.substring(1, tagsToSplit.length() - 1).split(TAG_DELIMITER_REGEX);
    }
}
