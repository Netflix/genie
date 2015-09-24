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

import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
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
import javax.persistence.ManyToMany;
import javax.persistence.OrderColumn;
import javax.persistence.PostPersist;
import javax.persistence.PostUpdate;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Representation of the state of the Cluster object.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(name = "clusters")
@Cacheable(false)
public class ClusterEntity extends CommonFields {

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @NotNull(message = "No cluster status entered and is required.")
    private ClusterStatus status;

    @Basic(optional = false)
    @Column(name = "cluster_type", nullable = false, length = 255)
    @NotBlank(message = "No cluster type entered and is required.")
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String clusterType;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "cluster_configs",
            joinColumns = @JoinColumn(name = "cluster_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 255)
    private Set<String> configs = new HashSet<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "cluster_tags",
            joinColumns = @JoinColumn(name = "cluster_id", referencedColumnName = "id")
    )
    @Column(name = "tag", nullable = false, length = 255)
    private Set<String> tags = new HashSet<>();

    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
            name = "clusters_commands",
            joinColumns = {
                    @JoinColumn(name = "cluster_id", referencedColumnName = "id", nullable = false)
            },
            inverseJoinColumns = {
                    @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false)
            }
    )
    @OrderColumn(name = "command_order", nullable = false)
    private List<CommandEntity> commands = new ArrayList<>();

    /**
     * Default Constructor.
     */
    public ClusterEntity() {
        super();
    }

    /**
     * Construct a new Cluster.
     *
     * @param name        The name of the cluster. Not null/empty/blank.
     * @param user        The user who created the cluster. Not null/empty/blank.
     * @param version     The version of the cluster. Not null/empty/blank.
     * @param status      The status of the cluster. Not null.
     * @param clusterType The type of the cluster. Not null/empty/blank.
     */
    public ClusterEntity(
            final String name,
            final String user,
            final String version,
            final ClusterStatus status,
            final String clusterType) {
        super(name, user, version);
        this.status = status;
        this.clusterType = clusterType;
    }

    /**
     * Check to make sure everything is OK before persisting.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @PostPersist
    @PostUpdate
    protected void onCreateOrUpdateCluster() throws GeniePreconditionException {
        this.addAndValidateSystemTags(this.tags);
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
     * Get the clusterType for this cluster.
     *
     * @return clusterType: The type of the cluster like yarn, presto, mesos
     * etc.
     */
    public String getClusterType() {
        return clusterType;
    }

    /**
     * Set the type for this cluster.
     *
     * @param clusterType The type of this cluster. Not null/empty/blank.
     */
    public void setClusterType(final String clusterType) {
        this.clusterType = clusterType;
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
     * Gets the tags allocated to this cluster.
     *
     * @return the tags
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this cluster.
     *
     * @param tags the tags to set. Not Null.
     */
    public void setTags(final Set<String> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Sets the commands for this cluster.
     *
     * @param commands The commands that this cluster supports
     */
    public void setCommands(final List<CommandEntity> commands) {
        //Clear references to this cluster in existing commands
        if (this.commands != null) {
            this.commands
                    .stream()
                    .filter(command -> command.getClusters() != null)
                    .forEach(command -> command.getClusters().remove(this));
        }
        //set the commands for this command
        this.commands = commands;

        //Add the reference in the new commands
        if (this.commands != null) {
            for (final CommandEntity command : this.commands) {
                Set<ClusterEntity> clusterEntities = command.getClusters();
                if (clusterEntities == null) {
                    clusterEntities = new HashSet<>();
                    command.setClusters(clusterEntities);
                }
                if (!clusterEntities.contains(this)) {
                    clusterEntities.add(this);
                }
            }
        }
    }

    /**
     * Add a new command to this cluster. Manages both sides of relationship.
     *
     * @param command The command to add. Not null.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void addCommand(final CommandEntity command)
            throws GeniePreconditionException {
        if (command == null) {
            throw new GeniePreconditionException("No command entered unable to add.");
        }

        if (this.commands == null) {
            this.commands = new ArrayList<>();
        }
        this.commands.add(command);

        Set<ClusterEntity> clusterEntities = command.getClusters();
        if (clusterEntities == null) {
            clusterEntities = new HashSet<>();
            command.setClusters(clusterEntities);
        }
        clusterEntities.add(this);
    }

    /**
     * Remove an command from this command. Manages both sides of relationship.
     *
     * @param command The command to remove. Not null.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void removeCommand(final CommandEntity command)
            throws GeniePreconditionException {
        if (command == null) {
            throw new GeniePreconditionException("No command entered unable to remove.");
        }

        if (this.commands != null) {
            this.commands.remove(command);
        }
        if (command.getClusters() != null) {
            command.getClusters().remove(this);
        }
    }

    /**
     * Remove all the commands from this application.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void removeAllCommands() throws GeniePreconditionException {
        if (this.commands != null) {
            final List<CommandEntity> locCommandEntities = new ArrayList<>();
            locCommandEntities.addAll(this.commands);
            for (final CommandEntity command : locCommandEntities) {
                this.removeCommand(command);
            }
        }
    }

    /**
     * Get a DTO from the contents of this entity.
     *
     * @return The DTO
     */
    public com.netflix.genie.common.dto.Cluster getDTO() {
        return new com.netflix.genie.common.dto.Cluster.Builder(
                this.getName(),
                this.getUser(),
                this.getVersion(),
                this.status,
                this.clusterType
        )
                .withId(this.getId())
                .withCreated(this.getCreated())
                .withUpdated(this.getUpdated())
                .withDescription(this.getDescription())
                .withTags(this.tags)
                .withConfigs(this.configs)
                .build();
    }
}
