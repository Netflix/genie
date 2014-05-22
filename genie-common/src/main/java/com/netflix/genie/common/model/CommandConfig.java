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
import java.util.ArrayList;
import java.util.Iterator;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Representation of the state of the Command Object.
 *
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class CommandConfig implements Serializable {

    private static final long serialVersionUID = -6106046473373305992L;

    /**
     * Unique ID to represent a row in database - e.g. prodhive11_mr1, prodhive13_tez, hadoop24
     */
    @Id
    private String id;

    /**
     * Name of this command - e.g. prodhive, pig, hadoop etc.
     */
    @Basic
    private String name;

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic
    private String status;

    /**
     * Location of the executable for this command.
     */
    @Basic
    private String executable;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    private String envPropFile;

    /**
     * Reference to all the config (xml's) needed for this command.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    private ArrayList<String> configs;

    /*
     * A list of id's of all the Applications that this command supports. This is needed
     * to fetch each application from the database in the entity manager context so that it
     * can be added to the command object before persistence.
     */
    @Transient
    private ArrayList<String> appIds;

    /**
     * Set of applications that can run this command - foreign key in database,
     * implemented by openjpa using join table
     * CommandConfig_ApplicationConfig.
     */
    @ManyToMany(targetEntity = ApplicationConfig.class, fetch = FetchType.EAGER)
    private ArrayList<ApplicationConfig> applications;

    /**
     * User who created this command.
     */
    @Basic
    private String user;

    /**
     * Job type of the command. eg: hive, pig , hadoop etc.
     */
    @Basic
    private String jobType;

    /**
     * Version number for this command.
     */
    @Basic
    private String version;

    /**
     * When was this created?
     */
    @Basic
    private Long createTime;

    /**
     * When was this last updated?
     */
    @Basic
    private Long updateTime;

    /**
     * Gets the id (primary key) for this command.
     *
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id (primary key) for this command.
     *
     * @param id unique id for this command
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the name for this command.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this command.
     *
     * @param name unique id for this cluster
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the status for this command.
     *
     * @return status - possible values: Types.ConfigStatus
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status for this application.
     *
     * @param status possible values Types.ConfigStatus
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Gets the executable for this command.
     *
     * @return executable -- full path on the node
     */
    public String getExecutable() {
        return executable;
    }

    /**
     * Sets the executable for this command.
     *
     * @param executable Full path of the executable on the node
     */
    public void setExecutable(String executable) {
        this.executable = executable;
    }

    /**
     * Gets the configs for this command.
     *
     * @return configs
     */
    public ArrayList<String> getConfigs() {
        return configs;
    }

    /**
     * Sets the configs for this command.
     *
     * @param configs The config files that this command needs
     */
    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    /**
     * Gets the applications that this command supports.
     *
     * @return applications Not supposed to be exposed in request/response hence
     * marked transient.
     */
    @XmlTransient
    public ArrayList<ApplicationConfig> getApplications() {
        return applications;
    }

    /**
     * Sets the applications for this command.
     *
     * @param applications The applications that this command supports
     */
    public void setApplications(ArrayList<ApplicationConfig> applications) {
        this.applications = applications;
    }

    /**
     * Gets the user that created this command.
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the user who created this command.
     *
     * @param user user who created this command
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Gets the type of the command.
     *
     * @return jobType --- for eg: hive, pig, presto
     */
    public String getJobType() {
        return jobType;
    }

    /**
     * Sets the job type for this command.
     *
     * @param jobType job type for this command
     */
    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    /**
     * Gets the version of this command.
     *
     * @return version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the version for this command.
     *
     * @param version version number for this command
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Gets the create time for this command.
     *
     * @return createTime - epoch time of creation in milliseconds
     *
     */
    public Long getCreateTime() {
        return createTime;
    }

    /**
     * Sets the create time for this command.
     *
     * @param createTime epoch time in ms
     */
    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    /**
     * Gets the last updated time for this command.
     *
     * @return updateTime - epoch time of update in milliseconds
     */
    public Long getUpdateTime() {
        return updateTime;
    }

    /**
     * Sets the updated time for this comamnd.
     *
     * @param updateTime epoch time in milliseconds
     */
    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * Gets the application id's supported by this command.
     *
     * @return appIds - a list of all application id's supported by this command
     */
    @XmlElement
    public ArrayList<String> getAppids() {
        if (this.applications != null) {
            appIds = new ArrayList<String>();
            Iterator<ApplicationConfig> it = this.applications.iterator();
            while (it.hasNext()) {
                appIds.add(((ApplicationConfig) it.next()).getId());
            }
        }
        return appIds;
    }

    /**
     * Sets the application id's for this command in string form.
     *
     * @param appIds list of application id's for this command
     */
    public void setAppids(ArrayList<String> appIds) {
        this.appIds = appIds;
    }

    /**
     * Gets the envPropFile name.
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getEnvPropFile() {
        return envPropFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile contains the list of env variables to set while
     * running this command.
     */
    public void setEnvPropFile(String envPropFile) {
        this.envPropFile = envPropFile;
    }
}
