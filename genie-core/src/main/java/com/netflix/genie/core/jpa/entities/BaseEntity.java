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

import com.netflix.genie.core.jpa.entities.projections.BaseProjection;
import com.netflix.genie.core.jpa.entities.projections.IdProjection;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;
import java.io.Serializable;
import java.util.Date;

/**
 * Abstract class to support basic columns for all entities for genie.
 *
 * @author tgianos
 */
@Getter
@ToString
@MappedSuperclass
public class BaseEntity implements Serializable, IdProjection, BaseProjection {

    private static final long serialVersionUID = 7526472297322776147L;

    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false, updatable = false)
    private long id;

    @Basic(optional = false)
    @Column(name = "created", nullable = false, updatable = false)
    @Temporal(TemporalType.TIMESTAMP)
    // TODO: Test out this instead of all the pre-persist stuff
//    @CreatedDate
    private Date created = new Date();

    @Basic(optional = false)
    @Column(name = "updated", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    // TODO: Test out this instead of all the pre-persist stuff
//    @LastModifiedDate
    private Date updated = new Date();

    @Version
    @Column(name = "entity_version", nullable = false)
    @Getter(AccessLevel.NONE)
    private Integer entityVersion;

    /**
     * Updates the created and updated timestamps to be creation time.
     */
    @PrePersist
    protected void onCreateBaseEntity() {
        final Date date = new Date();
        this.updated = date;
        this.created = date;
    }

    /**
     * On any update to the entity will update the update time.
     */
    @PreUpdate
    protected void onUpdateBaseEntity() {
        this.updated = new Date();
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
     * Get the time this entity was updated.
     *
     * @return The updated timestamp
     */
    public Date getUpdated() {
        return new Date(this.updated.getTime());
    }
}
