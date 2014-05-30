/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import java.util.Date;
import javax.persistence.Basic;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * Abstract class to support basic columns for all entities for genie.
 *
 * @author tgianos
 */
@MappedSuperclass
public class Auditable {

    /**
     * Default constructor.
     */
    public Auditable() {
    }

    /**
     * Unique ID.
     */
    @Id
    private String id;

    /**
     * The creation timestamp.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Basic(optional = false)
    private Date created;

    /**
     * The update timestamp.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Basic(optional = false)
    private Date updated;

    /**
     * Updates the created and updated timestamps to be creation time.
     */
    @PrePersist
    protected void onCreate() {
        final Date date = new Date();
        this.updated = date;
        this.created = date;
    }

    /**
     * On any update to the entity will update the update time.
     */
    @PreUpdate
    protected void onUpdate() {
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
     * @param id The id to set
     */
    public void setId(final String id) {
        this.id = id;
    }

    /**
     * Get when this entity was created.
     *
     * @return The created timestamps
     */
    public Date getCreated() {
        return this.created;
    }

    /**
     * Set the created timestamp.
     *
     * @param created The created timestamp
     */
    public void setCreated(Date created) {
        this.created = created;
    }

    /**
     * Get the time this entity was updated.
     *
     * @return The updated timestamp
     */
    public Date getUpdated() {
        return this.updated;
    }

    /**
     * Set the time this entity was updated.
     *
     * @param updated The updated timestamp
     */
    public void setUpdated(final Date updated) {
        this.updated = updated;
    }
}
