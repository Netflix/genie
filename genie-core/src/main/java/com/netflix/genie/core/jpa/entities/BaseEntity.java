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

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;
import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/**
 * Abstract class to support basic columns for all entities for genie.
 *
 * @author tgianos
 */
@MappedSuperclass
@Slf4j
public class BaseEntity implements Serializable {

    private static final long serialVersionUID = 7526472297322776147L;

    @Id
    @Column(name = "id", updatable = false)
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String id;

    @Basic(optional = false)
    @Column(name = "created", nullable = false, updatable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created = new Date();

    @Basic(optional = false)
    @Column(name = "updated", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated = new Date();

    @Version
    @Column(name = "entity_version", nullable = false)
    private Long entityVersion;

    /**
     * Updates the created and updated timestamps to be creation time.
     */
    @PrePersist
    protected void onCreateBaseEntity() {
        final Date date = new Date();
        this.updated = date;
        this.created = date;

        //Make sure we have an id if one wasn't entered beforehand
        if (this.id == null) {
            this.id = UUID.randomUUID().toString();
        }
    }

    /**
     * On any update to the entity will update the update time.
     */
    @PreUpdate
    protected void onUpdateBaseEntity() {
        this.updated = new Date();
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
     * Set the id.
     *
     * @param id The id to set. Not null/empty/blank.
     * @throws GeniePreconditionException When precondition isn't met.
     */
    public void setId(final String id) throws GeniePreconditionException {
        if (StringUtils.isBlank(this.id)) {
            this.id = id;
        } else {
            throw new GeniePreconditionException("Id already set for this entity.");
        }
    }

    /**
     * Get when this entity was created.
     *
     * @return The created timestamps
     */
    public Date getCreated() {
        return new Date(this.created.getTime());
    }

    /**
     * Set the created timestamp. This is a No-Op. Set once by system.
     *
     * @param created The created timestamp
     */
    public void setCreated(final Date created) {
        log.debug("Tried to set created to {} for entity {}. Will not be persisted.", created, this.id);
        if (created.before(this.created)) {
            this.created = new Date(created.getTime());
        }
    }

    /**
     * Get the time this entity was updated.
     *
     * @return The updated timestamp
     */
    public Date getUpdated() {
        return new Date(this.updated.getTime());
    }

    /**
     * Set the time this entity was updated. This is a No-Op. Updated automatically by system.
     *
     * @param updated The updated timestamp
     */
    public void setUpdated(final Date updated) {
        this.updated = new Date(updated.getTime());
    }

    /**
     * Get the version of this entity.
     *
     * @return The entityVersion of this entity as handled by JPA
     */
    public Long getEntityVersion() {
        return entityVersion;
    }

    /**
     * Set the version of this entity. Shouldn't be called. Handled by JPA.
     *
     * @param entityVersion The new entityVersion
     */
    protected void setEntityVersion(final Long entityVersion) {
        this.entityVersion = entityVersion;
    }
}
