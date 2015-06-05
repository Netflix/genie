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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
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
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;

/**
 * Representation of the state of the Command Object.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Cacheable(false)
@ApiModel(description = "An entity for managing a Command in the Genie system.")
public class Command extends CommonEntityFields {

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic(optional = false)
    @Enumerated(EnumType.STRING)
    @ApiModelProperty(
            value = "The status of the command",
            required = true
    )
    @NotNull(message = "No command status entered and is required.")
    private CommandStatus status;

    /**
     * Location of the executable for this command.
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "Location of the executable for this command",
            required = true
    )
    @NotBlank(message = "No executable entered for command and is required.")
    private String executable;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    @ApiModelProperty(
            value = "Location of a property file which will be downloaded and sourced before command execution"
    )
    private String envPropFile;

    /**
     * Job type of the command. eg: hive, pig , hadoop etc.
     */
    @Basic
    @ApiModelProperty(
            value = "Job type of the command. eg: hive, pig , hadoop etc"
    )
    private String jobType;

    /**
     * Reference to all the configuration (xml's) needed for this command.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "Locations of all the configuration files needed for this command which will be downloaded"
    )
    private Set<String> configs;

    /**
     * Set of tags for a command.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "All the tags associated with this command",
            required = true
    )
    private Set<String> tags;

    /**
     * Set of applications that can run this command.
     */
    @JsonIgnore
    @ManyToOne
    private Application application;

    /**
     * The clusters this command is available on.
     */
    @JsonIgnore
    @ManyToMany(mappedBy = "commands", fetch = FetchType.LAZY)
    private Set<Cluster> clusters;

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
     * @param version The version of this command. Not null/empty/blank.
     * @param status The status of the command. Not null.
     * @param executable The executable of the command. Not null/empty/blank.
     */
    public Command(
            final String name,
            final String user,
            final String version,
            final CommandStatus status,
            final String executable) {
        super(name, user, version);
        this.status = status;
        this.executable = executable;
    }

    /**
     * Check to make sure everything is OK before persisting.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateCommand() throws GeniePreconditionException {
        // Add the id to the tags
        if (this.tags == null) {
           this.tags = new HashSet<>();
        }
        this.addAndValidateSystemTags(this.tags);
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
     * @param status The new status.
     * @see CommandStatus
     */
    public void setStatus(final CommandStatus status) {
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
     * @param executable Full path of the executable on the node.
     */
    public void setExecutable(final String executable) {
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
     * @return the tags
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this command.
     *
     * @param tags the tags to set.
     */
    public void setTags(final Set<String> tags) {
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
                commands = new HashSet<>();
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
}
