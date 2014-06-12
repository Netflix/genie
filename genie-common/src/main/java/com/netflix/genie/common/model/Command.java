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
import com.netflix.genie.common.model.Types.CommandStatus;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Column;
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
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the state of the Command Object.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Command extends Auditable implements Serializable {

    private static final long serialVersionUID = -6106046473373305992L;
    private static final Logger LOG = LoggerFactory.getLogger(Command.class);

    /**
     * Name of this command - e.g. prodhive, pig, hadoop etc.
     */
    @Basic(optional = false)
    private String name;

    /**
     * User who created this command.
     */
    @Basic(optional = false)
    private String user;

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic(optional = false)
    @Enumerated(EnumType.STRING)
    private CommandStatus status;

    /**
     * Location of the executable for this command.
     */
    @Basic(optional = false)
    private String executable;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    private String envPropFile;

    /**
     * Job type of the command. eg: hive, pig , hadoop etc.
     */
    @Basic
    private String jobType;

    /**
     * Version number for this command.
     */
    @Basic
    @Column(name = "columnVersion")
    private String version;

    /**
     * Reference to all the configuration (xml's) needed for this command.
     */
    @XmlElementWrapper(name = "configs")
    @XmlElement(name = "config")
    @ElementCollection(fetch = FetchType.EAGER)
    private Set<String> configs = new HashSet<String>();

    /**
     * Set of applications that can run this command.
     */
    @XmlElementWrapper(name = "applications")
    @XmlElement(name = "application")
    @ManyToMany(fetch = FetchType.EAGER)
    private Set<Application> applications = new HashSet<Application>();

    /**
     * The clusters this command is available on.
     */
    @XmlTransient
    @JsonIgnore
    @ManyToMany(mappedBy = "commands", fetch = FetchType.LAZY)
    private Set<Cluster> clusters = new HashSet<Cluster>();

    /**
     * Default Constructor.
     */
    protected Command() {
        super();
    }

    /**
     * Construct a new Command with all required parameters.
     *
     * @param name The name of the command. Not null/empty/blank.
     * @param user The user who created the command. Not null/empty/blank.
     * @param status The status of the command. Not null.
     * @param executable The executable of the command. Not null/empty/blank.
     * @throws CloudServiceException
     */
    public Command(
            final String name,
            final String user,
            final CommandStatus status,
            final String executable) throws CloudServiceException {
        super();
        validate(name, user, status, executable);
        this.name = name;
        this.user = user;
        this.status = status;
        this.executable = executable;
    }

    /**
     * Gets the name for this command.
     *
     * @return name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets the name for this command.
     *
     * @param name unique id for this cluster. Not null/empty/blank.
     * @throws CloudServiceException
     */
    public void setName(final String name) throws CloudServiceException {
        validate(name, this.user, this.status, this.executable);
        this.name = name;
    }

    /**
     * Gets the user that created this command.
     *
     * @return user
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the user who created this command.
     *
     * @param user user who created this command. Not null/empty/blank.
     * @throws CloudServiceException
     */
    public void setUser(final String user) throws CloudServiceException {
        validate(this.name, user, this.status, this.executable);
        this.user = user;
    }

    /**
     * Gets the status for this command.
     *
     * @return the status
     * @see CommandStatus
     */
    public CommandStatus getStatus() {
        return this.status;
    }

    /**
     * Sets the status for this application.
     *
     * @param status The new status. Not null.
     * @throws CloudServiceException
     * @see CommandStatus
     */
    public void setStatus(final CommandStatus status) throws CloudServiceException {
        validate(this.name, this.user, status, this.executable);
        this.status = status;
    }

    /**
     * Gets the executable for this command.
     *
     * @return executable -- full path on the node
     */
    public String getExecutable() {
        return this.executable;
    }

    /**
     * Sets the executable for this command.
     *
     * @param executable Full path of the executable on the node. Not
     * null/empty/blank.
     * @throws CloudServiceException
     */
    public void setExecutable(final String executable) throws CloudServiceException {
        validate(this.name, this.user, this.status, this.executable);
        this.executable = executable;
    }

    /**
     * Gets the envPropFile name.
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getEnvPropFile() {
        return this.envPropFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile contains the list of env variables to set while
     * running this command.
     */
    public void setEnvPropFile(final String envPropFile) {
        this.envPropFile = envPropFile;
    }

    /**
     * Gets the type of the command.
     *
     * @return jobType --- for eg: hive, pig, presto
     */
    public String getJobType() {
        return this.jobType;
    }

    /**
     * Sets the job type for this command.
     *
     * @param jobType job type for this command
     */
    public void setJobType(final String jobType) {
        this.jobType = jobType;
    }

    /**
     * Gets the version of this command.
     *
     * @return version
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Sets the version for this command.
     *
     * @param version version number for this command
     */
    public void setVersion(final String version) {
        this.version = version;
    }

    /**
     * Gets the configurations for this command.
     *
     * @return the configurations
     */
    public Set<String> getConfigs() {
        return this.configs;
    }

    /**
     * Sets the configurations for this command.
     *
     * @param configs The configuration files that this command needs
     * @throws CloudServiceException
     */
    public void setConfigs(final Set<String> configs) throws CloudServiceException {
        if (configs == null) {
            final String msg = "No configurations passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        this.configs = configs;
    }

    /**
     * Gets the applications that this command supports.
     *
     * @return applications
     */
    public Set<Application> getApplications() {
        return this.applications;
    }

    /**
     * Sets the applications for this command.
     *
     * @param applications The applications that this command supports
     * @throws CloudServiceException
     */
    public void setApplications(final Set<Application> applications) throws CloudServiceException {
        if (applications == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No applications passed in to set. Unable to continue.");
        }
        this.applications = applications;
    }

    /**
     * Get the clusters this command is available on.
     *
     * @return The clusters.
     */
    public Set<Cluster> getClusters() {
        return this.clusters;
    }

    /**
     * Set the clusters this command is available on.
     *
     * @param clusters the clusters
     * @throws CloudServiceException
     */
    public void setClusters(Set<Cluster> clusters) throws CloudServiceException {
        if (clusters == null) {
            final String msg = "No clusters passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        this.clusters = clusters;
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param command The configuration to check
     * @throws CloudServiceException
     */
    public static void validate(final Command command) throws CloudServiceException {
        if (command == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command entered to validate");
        }
        validate(
                command.getName(),
                command.getUser(),
                command.getStatus(),
                command.getExecutable());
    }

    /**
     * Helper method for checking the validity of required parameters.
     *
     * @param name The name of the command
     * @param user The user who created the command
     * @param status The status of the command
     * @throws CloudServiceException
     */
    private static void validate(
            final String name,
            final String user,
            final CommandStatus status,
            final String executable)
            throws CloudServiceException {
        final StringBuilder builder = new StringBuilder();
        if (StringUtils.isBlank(user)) {
            builder.append("User name is missing and is required.\n");
        }
        if (StringUtils.isBlank(name)) {
            builder.append("Command name is missing and is required.\n");
        }
        if (status == null) {
            builder.append("No command status entered and is required.\n");
        }
        if (StringUtils.isBlank(executable)) {
            builder.append("No executable entered for command and is required.\n");
        }

        if (builder.length() != 0) {
            builder.insert(0, "Command configuration errors:\n");
            final String msg = builder.toString();
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }
}
