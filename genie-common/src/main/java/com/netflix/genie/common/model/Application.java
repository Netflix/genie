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

import com.netflix.genie.common.model.Types.ApplicationStatus;
import java.io.Serializable;
import java.util.ArrayList;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Table;

/**
 * Representation of the state of Application Config object.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class Application extends Auditable implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Name of this application - e.g. mapredue1, mapreduce2, tez etc.
     */
    @Basic(optional = false)
    private String name;

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Enumerated(EnumType.STRING)
    private ApplicationStatus status;

    /**
     * Reference to all the config's needed for this application.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    private ArrayList<String> configs;

    /**
     * Set of jars required for this application.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    private ArrayList<String> jars;

    /**
     * User who created this application.
     */
    @Basic(optional = false)
    private String user;

    /**
     * Version number for this application.
     */
    @Basic
    private String version;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    private String envPropFile;

    /**
     * Default constructor.
     */
    public Application() {
        super();
    }

    /**
     * Gets the name for this application.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this application.
     *
     * @param name unique id for this cluster
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the status for this application.
     *
     * @return status
     * @see ApplicationStatus
     */
    public ApplicationStatus getStatus() {
        return status;
    }

    /**
     * Sets the status for this application.
     *
     * @param status One of the possible statuses
     */
    public void setStatus(final ApplicationStatus status) {
        this.status = status;
    }

    /**
     * Gets the configs for this application.
     *
     * @return configs
     */
    public ArrayList<String> getConfigs() {
        return configs;
    }

    /**
     * Sets the configs for this application.
     *
     * @param configs The config files that this application needs
     */
    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    /**
     * Gets the jars for this application.
     *
     * @return jars list of jars this application relies on for execution
     */
    public ArrayList<String> getJars() {
        return jars;
    }

    /**
     * Sets the jars needed for this application.
     *
     * @param jars All jars needed for execution of this application
     */
    public void setJars(ArrayList<String> jars) {
        this.jars = jars;
    }

    /**
     * Gets the user that created this application.
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the user who created this application.
     *
     * @param user user who created this application
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Gets the version of this application.
     *
     * @return version - like 1.2.3
     *
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the version for this application.
     *
     * @param version version number for this application
     */
    public void setVersion(String version) {
        this.version = version;
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
     * running a command using this application.
     */
    public void setEnvPropFile(String envPropFile) {
        this.envPropFile = envPropFile;
    }
}
