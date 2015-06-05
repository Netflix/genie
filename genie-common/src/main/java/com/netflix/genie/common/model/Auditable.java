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
package com.netflix.genie.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.util.JsonDateDeserializer;
import com.netflix.genie.common.util.JsonDateSerializer;
import com.wordnik.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Date;
import java.util.UUID;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class to support basic columns for all entities for genie.
 *
 * @author tgianos
 */
@MappedSuperclass
public class Auditable implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Auditable.class);
    private static final long serialVersionUID = 7526472297322776147L;

    /**
     * Unique ID.
     */
    @Id
    @Column(updatable = false)
    @ApiModelProperty(
            value = "The unique id of this resource. If one is not provided it is created internally"
    )
    private String id;

    /**
     * The creation timestamp.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Basic(optional = false)
    @Column(updatable = false)
    @ApiModelProperty(
            value = "When this resource was created. Set automatically by system",
            readOnly = true,
            dataType = "dateTime"
    )
    @JsonSerialize(using = JsonDateSerializer.class)
    @JsonDeserialize(using = JsonDateDeserializer.class)
    private Date created = new Date();

    /**
     * The update timestamp.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Basic(optional = false)
    @ApiModelProperty(
            value = "When this resource was last updated. Set automatically by system",
            readOnly = true,
            dataType = "dateTime"
    )
    @JsonSerialize(using = JsonDateSerializer.class)
    @JsonDeserialize(using = JsonDateDeserializer.class)
    private Date updated = new Date();

    /**
     * The version of this entity. Auto handled by JPA.
     */
    @JsonIgnore
    @Version
    @Column(name = "entityVersion")
    private Long entityVersion;

    /**
     * Updates the created and updated timestamps to be creation time.
     */
    @PrePersist
    protected void onCreateAuditable() {
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
    protected void onUpdateAuditable() {
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
        LOG.debug("Tried to set created to " + created + " for entity " + this.id + ". Will not be persisted.");
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

    /**
     * Convert this object to a string representation.
     *
     * @return This application data represented as a JSON structure
     */
    @Override
    public String toString() {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (final JsonProcessingException ioe) {
            LOG.error(ioe.getLocalizedMessage(), ioe);
            return ioe.getLocalizedMessage();
        }
    }
}
