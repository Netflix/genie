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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Types.ApplicationStatus;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the state of Application Configuration object.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Application extends Auditable implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

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
     * Reference to all the configurations needed for this application.
     */
    @XmlElementWrapper(name = "configs")
    @XmlElement(name = "config")
    @ElementCollection(fetch = FetchType.EAGER)
    private Set<String> configs = new HashSet<String>();

    /**
     * Set of jars required for this application.
     */
    @XmlElementWrapper(name = "jars")
    @XmlElement(name = "jar")
    @ElementCollection(fetch = FetchType.EAGER)
    private Set<String> jars = new HashSet<String>();

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
     * The commands this application is associated with.
     */
    @XmlTransient
    @JsonIgnore
    @ManyToMany(mappedBy = "applications", fetch = FetchType.LAZY)
    private Set<Command> commands = new HashSet<Command>();

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
    public void setName(final String name) {
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
     * Gets the configurations for this application.
     *
     * @return the configurations for this application
     */
    public Set<String> getConfigs() {
        return this.configs;
    }

    /**
     * Sets the configurations for this application.
     *
     * @param configs The configuration files that this application needs
     * @throws CloudServiceException
     */
    public void setConfigs(final Set<String> configs) throws CloudServiceException {
        if (configs == null || configs.isEmpty()) {
            final String msg = "No configs passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        this.configs = configs;
    }

    /**
     * Gets the jars for this application.
     *
     * @return list of jars this application relies on for execution
     */
    public Set<String> getJars() {
        return this.jars;
    }

    /**
     * Sets the jars needed for this application.
     *
     * @param jars All jars needed for execution of this application
     * @throws CloudServiceException
     */
    public void setJars(final Set<String> jars) throws CloudServiceException {
        if (jars == null) {
            final String msg = "No jars passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
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
    public void setUser(final String user) {
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
    public void setVersion(final String version) {
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
    public void setEnvPropFile(final String envPropFile) {
        this.envPropFile = envPropFile;
    }

    /**
     * Get all the commands associated with this application.
     *
     * @return The commands
     */
    public Set<Command> getCommands() {
        return this.commands;
    }

    /**
     * Set all the commands associated with this application.
     *
     * @param commands The commands to set.
     */
    public void setCommands(final Set<Command> commands) {
        this.commands = commands;
    }
}
