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
package com.netflix.genie.core.jpa.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.Basic;
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
import javax.persistence.OneToMany;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
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
public class CommandEntity extends CommonFields {

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @NotNull(message = "No command status entered and is required.")
    private CommandStatus status;

    @Basic(optional = false)
    @Column(name = "executable", nullable = false, length = 255)
    @NotBlank(message = "No executable entered for command and is required.")
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String executable;

    @Basic
    @Lob
    @Column(name = "setup_file")
    private String setupFile;

    @Basic
    @Column(name = "job_type", length = 255)
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String jobType;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "command_configs",
            joinColumns = @JoinColumn(name = "command_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 1024)
    private Set<String> configs = new HashSet<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "command_tags",
            joinColumns = @JoinColumn(name = "command_id", referencedColumnName = "id")
    )
    @Column(name = "tag", nullable = false, length = 255)
    private Set<String> tags = new HashSet<>();

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
    private Set<ApplicationEntity> applications = new HashSet<>();

    @ManyToMany(mappedBy = "commands", fetch = FetchType.LAZY)
    private Set<ClusterEntity> clusters = new HashSet<>();

    @OneToMany(mappedBy = "cluster", fetch = FetchType.LAZY)
    private Set<JobEntity> jobs = new HashSet<>();

    /**
     * Default Constructor.
     */
    public CommandEntity() {
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
    public CommandEntity(
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
     * @throws GenieException If any precondition isn't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateCommand() throws GenieException {
        this.setCommandTags(this.getFinalTags());
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
    public Set<ApplicationEntity> getApplications() {
        return this.applications;
    }

    /**
     * Sets the applications for this command.
     *
     * @param applications The application that this command uses
     */
    public void setApplications(final Set<ApplicationEntity> applications) {
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
     * Get the set of tags for this command.
     *
     * @return The command tags
     */
    public Set<String> getCommandTags() {
        return this.getSortedTags() == null
                ? Sets.newHashSet()
                : Sets.newHashSet(this.getSortedTags().split(COMMA));
    }

    /**
     * Set the tags for the command.
     *
     * @param commandTags The tags for the command
     */
    public void setCommandTags(final Set<String> commandTags) {
        this.tags.clear();
        this.setSortedTags(commandTags);
        if (commandTags != null) {
            this.tags.addAll(commandTags);
        }
    }

    /**
     * Gets the tags allocated to this command.
     *
     * @return the tags
     */
    protected Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this command.
     *
     * @param tags the tags to set.
     */
    protected void setTags(final Set<String> tags) {
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
    public Set<ClusterEntity> getClusters() {
        return this.clusters;
    }

    /**
     * Set the clusters this command is available on.
     *
     * @param clusters the clusters
     */
    protected void setClusters(final Set<ClusterEntity> clusters) {
        this.clusters.clear();
        if (clusters != null) {
            this.clusters.addAll(clusters);
        }
    }

    /**
     * Get the jobs which used this command. Probably shouldn't be used as it will return an enormous list.
     *
     * @return The jobs this command is used for
     */
    protected Set<JobEntity> getJobs() {
        return this.jobs;
    }

    /**
     * Add a job to the set of jobs this command was used for.
     *
     * @param job The job
     */
    public void addJob(final JobEntity job) {
        if (job != null) {
            this.jobs.add(job);
        }
    }

    /**
     * Set the jobs run using this command.
     *
     * @param jobs The jobs
     */
    public void setJobs(final Set<JobEntity> jobs) {
        this.jobs.clear();

        if (jobs != null) {
            this.jobs.addAll(jobs);
        }
    }

    /**
     * Get a dto based on the information in this entity.
     *
     * @return The dto
     */
    public Command getDTO() {
        return new Command.Builder(
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
