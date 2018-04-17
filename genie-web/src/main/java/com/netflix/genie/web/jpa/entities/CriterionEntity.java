/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.validation.constraints.Size;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Entity for criteria records.
 *
 * @author tgianos
 * @since 3.3.0
 */
@NoArgsConstructor
@Getter
@Setter
@ToString(callSuper = true, of = {"uniqueId", "name", "version", "status"}, doNotUseGetters = true)
@Entity
@Table(name = "criteria")
public class CriterionEntity extends IdEntity {
    @Basic
    @Column(name = "unique_id", updatable = false)
    @Size(max = 255, message = "The id part of the criterion can't be longer than 255 characters")
    private String uniqueId;

    @Basic
    @Column(name = "name", updatable = false)
    @Size(max = 255, message = "The name part of the criterion can't be longer than 255 characters")
    private String name;

    @Basic
    @Column(name = "version", updatable = false)
    @Size(max = 255, message = "The version part of the criterion can't be longer than 255 characters")
    private String version;

    @Basic
    @Column(name = "status", updatable = false)
    @Size(max = 255, message = "The status part of the criterion can't be longer than 255 characters")
    private String status;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "criteria_tags",
        joinColumns = {
            @JoinColumn(name = "criterion_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    private Set<TagEntity> tags = new HashSet<>();

    /**
     * Constructor.
     *
     * @param uniqueId The unique id of the resource this criterion is or was trying to match
     * @param name     The name of the resource this criterion is or was trying to match
     * @param version  The version of the resource this criterion is or was trying to match
     * @param status   The status of the resource this criterion is or was trying to match
     * @param tags     The tags on the resource this criterion is or was trying to match
     */
    public CriterionEntity(
        @Nullable final String uniqueId,
        @Nullable final String name,
        @Nullable final String version,
        @Nullable final String status,
        @Nullable final Set<TagEntity> tags
    ) {
        super();
        this.uniqueId = uniqueId;
        this.name = name;
        this.version = version;
        this.status = status;
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Get the unique id this criterion was using if there was one.
     *
     * @return The unique id wrapped in an {@link Optional}
     */
    public Optional<String> getUniqueId() {
        return Optional.ofNullable(this.uniqueId);
    }

    /**
     * Set the unique id this criterion used.
     *
     * @param uniqueId The unique id to set
     */
    public void setUniqueId(@Nullable final String uniqueId) {
        this.uniqueId = uniqueId;
    }

    /**
     * Get the name this criterion was using if there was one.
     *
     * @return The name wrapped in an {@link Optional}
     */
    public Optional<String> getName() {
        return Optional.ofNullable(this.name);
    }

    /**
     * Set the name this criterion used.
     *
     * @param name The name to set
     */
    public void setName(@Nullable final String name) {
        this.name = name;
    }

    /**
     * Get the version this criterion was using if there was one.
     *
     * @return The version wrapped in an {@link Optional}
     */
    public Optional<String> getVersion() {
        return Optional.ofNullable(this.version);
    }

    /**
     * Set the version this criterion used.
     *
     * @param version The version to set
     */
    public void setVersion(@Nullable final String version) {
        this.version = version;
    }

    /**
     * Get the status this criterion was using if there was one.
     *
     * @return The status wrapped in an {@link Optional}
     */
    public Optional<String> getStatus() {
        return Optional.ofNullable(this.status);
    }

    /**
     * Set the status this criterion used.
     *
     * @param status The version to set
     */
    public void setStatus(@Nullable final String status) {
        this.status = status;
    }

    /**
     * Set all the tags associated to this criterion.
     *
     * @param tags The criterion tags to set
     */
    public void setTags(@Nullable final Set<TagEntity> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
