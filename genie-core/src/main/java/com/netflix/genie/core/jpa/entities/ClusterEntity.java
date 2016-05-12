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

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;

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
import javax.persistence.OrderColumn;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Representation of the state of the Cluster object.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Entity
@Table(name = "clusters")
public class ClusterEntity extends SetupFileEntity {

    private static final long serialVersionUID = -5674870110962005872L;

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @NotNull(message = "No cluster status entered and is required.")
    private ClusterStatus status;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "cluster_configs",
        joinColumns = @JoinColumn(name = "cluster_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 1024)
    private Set<String> configs = new HashSet<>();

    // TODO: Make lazy?
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
        name = "clusters_commands",
        joinColumns = {
            @JoinColumn(name = "cluster_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @OrderColumn(name = "command_order", nullable = false)
    private List<CommandEntity> commands = new ArrayList<>();

    @OneToMany(mappedBy = "cluster", fetch = FetchType.LAZY)
    private Set<JobEntity> jobs = new HashSet<>();

    /**
     * Default Constructor.
     */
    public ClusterEntity() {
        super();
    }

    /**
     * Check to make sure everything is OK before persisting.
     *
     * @throws GenieException If any precondition isn't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateCluster() throws GenieException {
        this.setTags(this.getFinalTags());
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
     * @param status The status of the cluster. Not null.
     * @see ClusterStatus
     */
    public void setStatus(final ClusterStatus status) {
        this.status = status;
    }

    /**
     * Gets the configurations for this cluster.
     *
     * @return The cluster configurations as unmodifiable list
     */
    public Set<String> getConfigs() {
        return this.configs;
    }

    /**
     * Sets the configurations for this cluster.
     *
     * @param configs The configuration files that this cluster needs. Not
     *                null/empty.
     */
    public void setConfigs(final Set<String> configs) {
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Gets the commands that this cluster supports.
     *
     * @return commands Not supposed to be exposed in request/response messages
     * hence marked transient.
     */
    public List<CommandEntity> getCommands() {
        return this.commands;
    }

    /**
     * Sets the commands for this cluster.
     *
     * @param commands The commands that this cluster supports
     * @throws GeniePreconditionException If the commands are already added to the list
     */
    public void setCommands(final List<CommandEntity> commands) throws GeniePreconditionException {
        if (commands != null
            && commands.stream().map(CommandEntity::getId).distinct().count() != commands.size()) {
            throw new GeniePreconditionException("List of commands to set cannot contain duplicates");
        }

        //Clear references to this cluster in existing commands
        for (final CommandEntity command : this.commands) {
            command.getClusters().remove(this);
        }
        this.commands.clear();

        if (commands != null) {
            // Set the commands for this cluster
            this.commands.addAll(commands);

            //Add the reference in the new commands
            for (final CommandEntity command : this.commands) {
                command.getClusters().add(this);
            }
        }
    }

    /**
     * Add a new command to this cluster. Manages both sides of relationship.
     *
     * @param command The command to add. Not null.
     * @throws GeniePreconditionException If the command is a duplicate of an existing command
     */
    public void addCommand(@NotNull final CommandEntity command) throws GeniePreconditionException {
        if (
            this.commands
                .stream()
                .map(CommandEntity::getId)
                .filter(id -> id.equals(command.getId()))
                .count() != 0
            ) {
            throw new GeniePreconditionException("A command with id " + command.getId() + " is already added");
        }
        this.commands.add(command);
        command.getClusters().add(this);
    }

    /**
     * Remove a command from this cluster. Manages both sides of relationship.
     *
     * @param command The command to remove. Not null.
     */
    public void removeCommand(@NotNull final CommandEntity command) {
        this.commands.remove(command);
        command.getClusters().remove(this);
    }

    /**
     * Remove all the commands from this application.
     */
    public void removeAllCommands() {
        Lists.newArrayList(this.commands).forEach(this::removeCommand);
    }

    /**
     * Get the jobs which ran on this cluster. Probably shouldn't be used as it will return an enormous list.
     *
     * @return The jobs this cluster ran
     */
    protected Set<JobEntity> getJobs() {
        return this.jobs;
    }

    /**
     * Set the jobs run on this cluster.
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
     * Add a job to the set of jobs this cluster ran.
     *
     * @param job The job
     */
    public void addJob(final JobEntity job) {
        if (job != null) {
            this.jobs.add(job);
        }
    }

    /**
     * Get a DTO from the contents of this entity.
     *
     * @return The DTO
     */
    public Cluster getDTO() {
        return new Cluster.Builder(
            this.getName(),
            this.getUser(),
            this.getVersion(),
            this.status
        )
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .withDescription(this.getDescription())
            .withTags(this.getTags())
            .withSetupFile(this.getSetupFile())
            .withConfigs(this.configs)
            .build();
    }
}
