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

import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import lombok.Getter;
import lombok.Setter;
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
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.validation.constraints.Min;
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
@Getter
@Setter
public class CommandEntity extends SetupFileEntity {

    private static final long serialVersionUID = -8058995173025433517L;

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

    @Basic(optional = false)
    @Column(name = "check_delay", nullable = false)
    @Min(1)
    private long checkDelay = Command.DEFAULT_CHECK_DELAY;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "command_configs",
        joinColumns = @JoinColumn(name = "command_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 1024)
    private Set<String> configs = new HashSet<>();

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
     * Check to make sure everything is OK before persisting.
     *
     * @throws GenieException If any precondition isn't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateCommand() throws GenieException {
        this.setTags(this.getFinalTags());
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
        for (final ApplicationEntity application : this.applications) {
            application.getCommands().remove(this);
        }
        //set the application for this command
        this.applications.clear();
        if (applications != null) {
            this.applications.addAll(applications);
        }

        //Add the reverse reference in the new applications
        for (final ApplicationEntity application : this.applications) {
            application.getCommands().add(this);
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
            this.executable,
            this.checkDelay
        )
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .withDescription(this.getDescription())
            .withTags(this.getTags())
            .withConfigs(this.configs)
            .withSetupFile(this.getSetupFile())
            .build();
    }
}
