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
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
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
import javax.persistence.ManyToOne;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
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
@ApiModel(value = "A Command")
public class Command extends CommonEntityFields {

    private static final long serialVersionUID = -6106046473373305992L;
    private static final Logger LOG = LoggerFactory.getLogger(Command.class);

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic(optional = false)
    @Enumerated(EnumType.STRING)
    @ApiModelProperty(
            value = "If it is in use - ACTIVE, DEPRECATED, INACTIVE",
            required = true)
    private CommandStatus status;

    /**
     * Location of the executable for this command.
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "Location of the executable for this command",
            required = true)
    private String executable;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    @ApiModelProperty(
            value = "Users can specify a property file"
            + " location with environment variables")
    private String envPropFile;

    /**
     * Job type of the command. eg: hive, pig , hadoop etc.
     */
    @Basic
    @ApiModelProperty(
            value = "Job type of the command. eg: hive, pig , hadoop etc")
    private String jobType;

    /**
     * Reference to all the configuration (xml's) needed for this command.
     */
    @XmlElementWrapper(name = "configs")
    @XmlElement(name = "config")
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "Reference to all the configuration"
            + " files needed for this command")
    private Set<String> configs;

    /**
     * Set of applications that can run this command.
     */
    @ApiModelProperty(
            value = "The application this command uses.")
    @XmlTransient
    @JsonIgnore
    @ManyToOne
    private Application application;

    /**
     * The clusters this command is available on.
     */
    @XmlTransient
    @JsonIgnore
    @ManyToMany(mappedBy = "commands", fetch = FetchType.LAZY)
    private Set<Cluster> clusters;

    /**
     * Set of tags for a command.
     */
    @XmlElementWrapper(name = "tags")
    @XmlElement(name = "tag")
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "Reference to all the tags"
            + " associated with this command.")
    private Set<String> tags;

    /**
     * Default Constructor.
     */
    public Command() {
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
        super(name, user);
        this.status = status;
        this.executable = executable;
    }

    /**
     * Check to make sure everything is OK before persisting.
     *
     * @throws CloudServiceException
     */
    @PrePersist
    protected void onCreateCommand() throws CloudServiceException {
        validate(this.getName(), this.getUser(), this.status, this.executable);
        // Add the id to the tags
        if (this.tags == null) {
           this.tags = new HashSet<String>();
           this.tags.add(this.getId());
        }
    }

    /**
     * On any update to the command will add id to tags.
     */
    @PreUpdate
    protected void onUpdateCommand() {
        // Add the id to the tags
        this.tags.add(this.getId());
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
        if (status == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No status entered.");
        }
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
        if (StringUtils.isBlank(executable)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No executable entered.");
        }
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
     */
    public void setConfigs(final Set<String> configs) {
        this.configs = configs;
    }

    /**
     * Gets the application that this command uses.
     *
     * @return application
     */
    public Application getApplication() {
        return this.application;
    }

    /**
     * Gets the tags allocated to this command.
     *
     * @return the tags as an unmodifiable list
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this command.
     *
     * @param tags the tags to set. Not Null.
     * @throws CloudServiceException
     */
    public void setTags(final Set<String> tags) throws CloudServiceException {
        this.tags = tags;
    }

    /**
     * Sets the application for this command.
     *
     * @param application The application that this command uses
     */
    public void setApplication(final Application application) {
        //Clear references to this command in existing applications
        if (this.application != null
                && this.application.getCommands() != null) {
            this.application.getCommands().remove(this);
        }
        //set the application for this command
        this.application = application;

        //Add the reverse reference in the new applications
        if (this.application != null) {
            Set<Command> commands = this.application.getCommands();
            if (commands == null) {
                commands = new HashSet<Command>();
                this.application.setCommands(commands);
            }
            if (!commands.contains(this)) {
                commands.add(this);
            }
        }
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
     */
    protected void setClusters(final Set<Cluster> clusters) {
        this.clusters = clusters;
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param command The configuration to check
     * @throws CloudServiceException
     */
    public void validate() throws CloudServiceException {
        this.validate(
                this.getName(),
                this.getUser(),
                this.getStatus(),
                this.getExecutable());
    }

    /**
     * Helper method for checking the validity of required parameters.
     *
     * @param name The name of the command
     * @param user The user who created the command
     * @param status The status of the command
     * @throws CloudServiceException
     */
    private void validate(
            final String name,
            final String user,
            final CommandStatus status,
            final String executable)
            throws CloudServiceException {
        final StringBuilder builder = new StringBuilder();
        super.validate(builder, name, user);
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
