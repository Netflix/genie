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

import com.netflix.genie.common.model.Types.ClusterStatus;
import java.io.Serializable;
import java.util.ArrayList;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * Representation of the state of the Cluster object.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class Cluster extends Auditable implements Serializable {

    private static final long serialVersionUID = 8046582926818942370L;

    /**
     * Name for this cluster, e.g. cquery.
     */
    @Basic(optional = false)
    private String name;

    /**
     * User who created this cluster.
     */
    @Basic(optional = false)
    private String user;

    /**
     * Set of tags for scheduling - e.g. adhoc, sla, vpc etc.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    private ArrayList<String> tags;

    /**
     * Reference to all the config (xml's) needed for this cluster.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    private ArrayList<String> configs;

    /**
     * A list of id's of all the commands supported by this cluster.
     */
    @Transient
    private ArrayList<String> cmdIds;

    /**
     * Commands supported on this cluster - e.g. prodhive, testhive, etc.
     * Foreign Key in the database implemented by OpenJpa using join tables
     */
    @ManyToMany(targetEntity = Command.class, fetch = FetchType.EAGER)
    private ArrayList<Command> commands;

    /**
     * Version of this cluster.
     */
    @Basic
    private String version;

    /**
     * The class to use as the job manager for this cluster.
     */
    @Basic(optional = false)
    private String jobManager;

    /**
     * Status of cluster - UP, OUT_OF_SERVICE or TERMINATED.
     */
    @Enumerated(EnumType.STRING)
    private ClusterStatus status;

    /**
     * Default Constructor.
     */
    public Cluster() {
        super();
    }

    /**
     * Gets the name for this cluster.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this cluster.
     *
     * @param name unique id for this cluster
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the user that created this cluster.
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the user who created this cluster.
     *
     * @param user user who created this cluster
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Gets the tags allocated to this cluster.
     *
     * @return tags
     */
    public ArrayList<String> getTags() {
        return tags;
    }

    /**
     * Sets the tags allocated to this cluster.
     *
     * @param tags
     */
    public void setTags(ArrayList<String> tags) {
        this.tags = tags;
    }

    /**
     * Gets the configs for this cluster.
     *
     * @return The cluster configurations
     */
    public ArrayList<String> getConfigs() {
        return configs;
    }

    /**
     * Sets the configs for this cluster.
     *
     * @param configs The config files that this cluster needs
     */
    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    /**
     * Gets the commands that this cluster supports.
     *
     * @return commands Not supposed to be exposed in request/response messages
     * hence marked transient.
     */
    @XmlTransient
    public ArrayList<Command> getCommands() {
        return commands;
    }

    /**
     * Sets the commands for this cluster.
     *
     * @param commands The commands that this cluster supports
     */
    public void setCommands(ArrayList<Command> commands) {
        this.commands = commands;
    }

    /**
     * Gets the version of this cluster.
     *
     * @return version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the version for this cluster.
     *
     * @param version version number for this cluster
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Get the job manager class to use for this cluster.
     *
     * @return The class to use
     */
    public String getJobManager() {
        return this.jobManager;
    }

    /**
     * Set the job manager class to use for this cluster.
     *
     * @param jobManager The job manager class to use
     */
    public void setJobManager(final String jobManager) {
        this.jobManager = jobManager;
    }

    /**
     * Gets the status for this cluster.
     *
     * @return status - possible values: Types.ConfigStatus
     */
    public ClusterStatus getStatus() {
        return status;
    }

    /**
     * Sets the status for this cluster.
     *
     * @param status possible values Types.ConfigStatus
     */
    public void setStatus(final ClusterStatus status) {
        this.status = status;
    }

    /**
     * Gets the command id's supported by this cluster.
     *
     * @return cmdIds - a list of all command id's supported by this cluster
     */
    @XmlElement
    public ArrayList<String> getCmdIds() {
        if (this.commands != null) {
            this.cmdIds = new ArrayList<String>();
            for (final Command cce : this.commands) {
                this.cmdIds.add(cce.getId());
            }
        }
        return cmdIds;
    }

    /**
     * Sets the command id's for this cluster in string form.
     *
     * @param cmdIds list of command id's for this cluster
     */
    public void setCmdIds(ArrayList<String> cmdIds) {
        this.cmdIds = cmdIds;
    }
}
