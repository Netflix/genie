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
package com.netflix.genie.web.jpa.entities;

import com.netflix.genie.web.jpa.entities.projections.AuditProjection;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Version;
import java.time.Instant;

/**
 * Abstract class to support basic columns for all entities for genie.
 *
 * @author tgianos
 */
@Getter
@ToString(callSuper = true, of = {"created", "updated"})
@MappedSuperclass
public class AuditEntity extends IdEntity implements AuditProjection {

    @Basic(optional = false)
    @Column(name = "created", nullable = false, updatable = false)
    private Instant created = Instant.now();

    @Basic(optional = false)
    @Column(name = "updated", nullable = false)
    private Instant updated = Instant.now();

    @Version
    @Column(name = "entity_version", nullable = false)
    @Getter(AccessLevel.NONE)
    private Integer entityVersion;

    /**
     * Updates the created and updated timestamps to be creation time.
     */
    @PrePersist
    protected void onCreateBaseEntity() {
        final Instant now = Instant.now();
        this.updated = now;
        this.created = now;
    }

    /**
     * On any update to the entity will update the update time.
     */
    @PreUpdate
    protected void onUpdateBaseEntity() {
        this.updated = Instant.now();
    }

    /**
     * Get when this entity was created.
     *
     * @return The created timestamps
     */
    public Instant getCreated() {
        return this.created;
    }

    /**
     * Get the time this entity was updated.
     *
     * @return The updated timestamp
     */
    public Instant getUpdated() {
        return this.updated;
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
