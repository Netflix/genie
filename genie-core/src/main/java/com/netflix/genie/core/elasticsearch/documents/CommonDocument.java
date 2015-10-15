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
package com.netflix.genie.core.elasticsearch.documents;

import java.util.Date;

/**
 * The common entity fields for all Genie entities.
 *
 * @author amsharma
 * @author tgianos
 */
public class CommonDocument {

    private String id;
    private Date created;
    private Date updated;
    private String version;
    private String user;
    private String name;
    private String description;

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
        if (this.created != null) {
            return new Date(this.created.getTime());
        } else {
            return null;
        }
    }

    /**
     * Set the created timestamp. This is a No-Op. Set once by system.
     *
     * @param created The created timestamp
     */
    public void setCreated(final Date created) {
        this.created = new Date(created.getTime());
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
}
