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
import lombok.Getter;
import lombok.Setter;

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
@Getter
@Setter
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
     * Get a DTO from the contents of this entity.
     *
     * @return The DTO
     */
    public Cluster getDTO() {
        final Cluster.Builder builder = new Cluster.Builder(
            this.getName(),
            this.getUser(),
            this.getVersion(),
            this.status
        )
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .withTags(this.getTags())
            .withConfigs(this.configs);

        this.getDescription().ifPresent(builder::withDescription);
        this.getSetupFile().ifPresent(builder::withSetupFile);

        return builder.build();
    }
}
