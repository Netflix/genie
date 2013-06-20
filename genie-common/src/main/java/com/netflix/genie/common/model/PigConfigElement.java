/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Representation of the state of the Pig Config object.
 *
 * @author skrishnan
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class PigConfigElement implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    @Basic
    private String name;

    @Basic
    /* upper case in db - PROD, TEST or UNITTEST */
    private String type;

    @Basic
    /* upper case in db - ACTIVE, DEPRECATED, INACTIVE */
    private String status;

    @Basic
    private String s3PigProperties;

    @Basic
    private String user;

    @Basic
    private String pigVersion;

    @Basic
    private Long createTime;

    @Basic
    private Long updateTime;

    /**
     * Gets the id (primary key) for this config.
     *
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id (primary key) for this config.
     *
     * @param id
     *            unique id for this config
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get name for this config.
     *
     * @return name non-unique name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this config.
     *
     * @param name
     *            non-unique name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the type for this config.
     *
     * @return type - possible values Types.Configuration
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type for this config.
     *
     * @param type
     *            possible values Types.Configuration
     */
    public void setType(String type) {
        this.type = type.toUpperCase();
    }

    /**
     * Get status for config.
     *
     * @return status = ACTIVE, DEPRECATED, INACTIVE
     */
    public String getStatus() {
        return status;
    }

    /**
     * Set status for config.
     *
     * @param status
     *            - ACTIVE, DEPRECATED, INACTIVE
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Get user name who last updated this config.
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * Set user name to update config.
     *
     * @param user
     *            user name
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get location of the pig.properties for this config.
     *
     * @return (s3 or hdfs) location of pig.properties
     */
    public String getS3PigProperties() {
        return s3PigProperties;
    }

    /**
     * Set the location of pig.properties for this config.
     *
     * @param s3PigProperties
     *            (s3 or hdfs) location of pig.properties
     */
    public void setS3PigProperties(String s3PigProperties) {
        this.s3PigProperties = s3PigProperties;
    }

    /**
     * Get the version of pig.
     *
     * @return pigVersion
     */
    public String getPigVersion() {
        return pigVersion;
    }

    /**
     * Set the version of Pig.
     *
     * @param pigVersion
     *            version of Pig (e.g. 0.9.1)
     */
    public void setPigVersion(String pigVersion) {
        this.pigVersion = pigVersion;
    }

    /**
     * Get the create time for this config.
     *
     * @return createTime in ms
     */
    public Long getCreateTime() {
        return createTime;
    }

    /**
     * Set the create time for this config.
     *
     * @param createTime
     *            epoch time in ms
     */
    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    /**
     * Get the update time for this config.
     *
     * @return updateTime in ms
     */
    public Long getUpdateTime() {
        return updateTime;
    }

    /**
     * Set the update time for this config.
     *
     * @param updateTime
     *            epoch time in ms
     */
    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }
}
