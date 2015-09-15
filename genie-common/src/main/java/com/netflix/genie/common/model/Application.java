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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

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
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.PostPersist;
import javax.persistence.PostUpdate;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Set;

/**
 * Representation of the state of Application Configuration object.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(name = "applications")
@Cacheable(false)
@ApiModel(description = "An entity for managing an application in the Genie system.")
public class Application extends CommonFields {

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @ApiModelProperty(
            value = "The current status of this application",
            required = true
    )
    @NotNull(message = "No application status entered and is required.")
    private ApplicationStatus status;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Lob
    @Basic
    @Column(name = "setup_file")
    @ApiModelProperty(
            value = "A file location with environment variables or "
                    + "other settings which will be downloaded and sourced before application used"
    )
    private String setupFile;

    /**
     * Reference to all the configurations needed for this application.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "application_configs",
            joinColumns = @JoinColumn(name = "application_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 255)
    @ApiModelProperty(
            value = "All the configuration files needed for this application which will be downloaded pre-use"
    )
    private Set<String> configs = new HashSet<>();

    /**
     * Set of dependencies required for this application.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "application_dependencies",
            joinColumns = @JoinColumn(name = "application_id", referencedColumnName = "id")
    )
    @Column(name = "dependency", nullable = false, length = 255)
    @ApiModelProperty(
            value = "Any jars needed to run this application which will be downloaded pre use"
    )
    private Set<String> dependencies = new HashSet<>();

    /**
     * Set of tags for a application.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "application_tags",
            joinColumns = @JoinColumn(name = "application_id", referencedColumnName = "id")
    )
    @Column(name = "tag", nullable = false, length = 255)
    @ApiModelProperty(
            value = "The tags associated with this application",
            required = true
    )
    private Set<String> tags = new HashSet<>();

    /**
     * The commands this application is associated with.
     */
    @JsonIgnore
    @ManyToMany(mappedBy = "applications", fetch = FetchType.LAZY)
    private Set<Command> commands = new HashSet<>();

    /**
     * Default constructor.
     */
    public Application() {
        super();
    }

    /**
     * Construct a new Application with all required parameters.
     *
     * @param name    The name of the application. Not null/empty/blank.
     * @param user    The user who created the application. Not null/empty/blank.
     * @param version The version of this application. Not null/empty/blank.
     * @param status  The status of the application. Not null.
     */
    public Application(
            final String name,
            final String user,
            final String version,
            final ApplicationStatus status) {
        super(name, user, version);
        this.status = status;
    }

    /**
     * Check to make sure everything is OK before persisting.
     *
     * @throws GeniePreconditionException If any preconditions aren't met.
     */
    @PostPersist
    @PostUpdate
    protected void onCreateOrUpdateApplication() throws GeniePreconditionException {
        this.addAndValidateSystemTags(this.tags);
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
     * Gets the setupFile name.
     *
     * @return setupFile - file name containing environment variables.
     */
    public String getSetupFile() {
        return setupFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param setupFile location of a script to run while installing this application.
     */
    public void setSetupFile(final String setupFile) {
        this.setupFile = setupFile;
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
     */
    public void setConfigs(final Set<String> configs) {
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Gets the dependencies for this application.
     *
     * @return list of jars this application relies on for execution
     */
    public Set<String> getDependencies() {
        return this.dependencies;
    }

    /**
     * Sets the dependencies needed for this application.
     *
     * @param dependencies All dependencies needed for execution of this application
     */
    public void setDependencies(final Set<String> dependencies) {
        this.dependencies.clear();
        if (dependencies != null) {
            this.dependencies.addAll(dependencies);
        }
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
    //TODO: Add @Valid?
    protected void setCommands(final Set<Command> commands) {
        this.commands.clear();
        if (commands != null) {
            this.commands.addAll(commands);
        }
    }

    /**
     * Gets the tags allocated to this application.
     *
     * @return the tags
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this application.
     *
     * @param tags the tags to set. No tag can start with genie. as this is system reserved.
     */
    public void setTags(final Set<String> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }
}
