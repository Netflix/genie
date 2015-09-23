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
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.PostPersist;
import javax.persistence.PostUpdate;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Set;

/**
 * Representation of the state of the Command Object.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(name = "commands")
@Cacheable(false)
@ApiModel(description = "An entity for managing a Command in the Genie system.")
public class Command extends CommonFields {

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
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
    @Column(name = "executable", nullable = false, length = 255)
    @ApiModelProperty(
            value = "Location of the executable for this command",
            required = true
    )
    @NotBlank(message = "No executable entered for command and is required.")
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String executable;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    @Lob
    @Column(name = "setup_file")
    @ApiModelProperty(
            value = "Location of a setup file which will be downloaded and run before command execution"
    )
    private String setupFile;

    /**
     * Job type of the command. eg: hive, pig , hadoop etc.
     */
    @Basic
    @Column(name = "job_type", length = 255)
    @ApiModelProperty(
            value = "Job type of the command. eg: hive, pig , hadoop etc"
    )
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String jobType;

    /**
     * Reference to all the configuration (xml's) needed for this command.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "command_configs",
            joinColumns = @JoinColumn(name = "command_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 255)
    @ApiModelProperty(
            value = "Locations of all the configuration files needed for this command which will be downloaded"
    )
    private Set<String> configs = new HashSet<>();

    /**
     * Set of tags for a command.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "command_tags",
            joinColumns = @JoinColumn(name = "command_id", referencedColumnName = "id")
    )
    @Column(name = "tag", nullable = false, length = 255)
    @ApiModelProperty(
            value = "All the tags associated with this command",
            required = true
    )
    private Set<String> tags = new HashSet<>();

    /**
     * Set of applications that can run this command.
     */
    @JsonIgnore
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
            name = "commands_applications",
            joinColumns = {
                    @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false)
            },
            inverseJoinColumns = {
                    @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false)
            }
    )
    private Set<Application> applications = new HashSet<>();

    /**
     * The clusters this command is available on.
     */
    @JsonIgnore
    @ManyToMany(mappedBy = "commands", fetch = FetchType.LAZY)
    private Set<Cluster> clusters = new HashSet<>();

    /**
     * Default Constructor.
     */
    public Command() {
        super();
    }

    /**
     * Construct a new Command with all required parameters.
     *
     * @param name       The name of the command. Not null/empty/blank.
     * @param user       The user who created the command. Not null/empty/blank.
     * @param version    The version of this command. Not null/empty/blank.
     * @param status     The status of the command. Not null.
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
    @PostPersist
    @PostUpdate
    protected void onCreateOrUpdateCommand() throws GeniePreconditionException {
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
     * @return setupFile - file location containing setup steps.
     */
    public String getSetupFile() {
        return this.setupFile;
    }

    /**
     * Sets the setup file for this command.
     *
     * @param setupFile The file to run during command setup
     */
    public void setSetupFile(final String setupFile) {
        this.setupFile = setupFile;
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
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Gets the applications that this command uses.
     *
     * @return applications linked to this command
     */
    public Set<Application> getApplications() {
        return this.applications;
    }

    /**
     * Sets the applications for this command.
     *
     * @param applications The application that this command uses
     */
    public void setApplications(final Set<Application> applications) {
        //Clear references to this command in existing applications
        if (this.applications != null) {
            this.applications.stream()
                    .filter(application -> application.getCommands() != null)
                    .forEach(application -> application.getCommands().remove(this));
        }
        //set the application for this command
        if (this.applications == null) {
            this.applications = new HashSet<>();
        }
        this.applications.clear();
        if (applications != null) {
            this.applications.addAll(applications);
        }

        //Add the reverse reference in the new applications
        this.applications.stream()
                .forEach(
                        application -> {
                            if (application.getCommands() == null) {
                                application.setCommands(new HashSet<>());
                            }
                            application.getCommands().add(this);
                        }
                );
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
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
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
        this.clusters.clear();
        if (clusters != null) {
            this.clusters.addAll(clusters);
        }
    }

    /**
     * Get a dto based on the information in this entity.
     *
     * @return The dto
     */
    public com.netflix.genie.common.dto.Command getDTO() {
        return new com.netflix.genie.common.dto.Command.Builder(
                this.getName(),
                this.getUser(),
                this.getVersion(),
                this.status,
                this.executable
        )
                .withId(this.getId())
                .withCreated(this.getCreated())
                .withUpdated(this.getUpdated())
                .withDescription(this.getDescription())
                .withTags(this.tags)
                .withConfigs(this.configs)
                .withSetupFile(this.setupFile)
                .withJobType(this.jobType)
                .build();
    }
}
