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

import com.netflix.genie.common.exceptions.GenieException;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.net.HttpURLConnection;
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
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class to support basic columns for all entities for genie.
 *
 * @author tgianos
 */
@MappedSuperclass
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@ApiModel(value = "An auditable item")
public class Auditable {

    private static final Logger LOG = LoggerFactory.getLogger(Auditable.class);

    /**
     * Default constructor.
     */
    public Auditable() {
    }

    /**
     * Unique ID.
     */
    @Id
    @ApiModelProperty(
            value = "id",
            notes = "The unique id of this resource. If one is not provided it is set internally.")
    private String id;

    /**
     * The creation timestamp.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Basic(optional = false)
    @ApiModelProperty(
            value = "created",
            notes = "When this resource was created.",
            dataType = "date")
    private Date created = new Date();

    /**
     * The update timestamp.
     */
    @Temporal(TemporalType.TIMESTAMP)
    @Basic(optional = false)
    @ApiModelProperty(
            value = "updated",
            notes = "When this resource was last updated.",
            dataType = "date")
    private Date updated = new Date();

    /**
     * The version of this entity. Auto handled by JPA.
     */
    @XmlTransient
    @JsonIgnore
    @Version
    @Column(name = "version")
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
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    public void setId(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No ID entered.");
        }
        if (StringUtils.isBlank(this.id)) {
            this.id = id;
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Id already set for this entity.");
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
     * Set the created timestamp.
     *
     * @param created The created timestamp
     */
    public void setCreated(final Date created) {
        //This is to prevent the create time from being updated after
        //an entity has been persisted and someone is just trying to
        //update another field in the entity
        if (created.before(this.created)) {
            this.created.setTime(created.getTime());
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
     * Set the time this entity was updated.
     *
     * @param updated The updated timestamp
     */
    public void setUpdated(final Date updated) {
        this.updated.setTime(updated.getTime());
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
        } catch (final IOException ioe) {
            LOG.error(ioe.getLocalizedMessage(), ioe);
            return ioe.getLocalizedMessage();
        }
    }
}
