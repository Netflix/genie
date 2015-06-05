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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.ManyToMany;
import javax.persistence.OrderColumn;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;

/**
 * Representation of the state of the Cluster object.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
@Entity
@Cacheable(false)
@ApiModel(description = "An entity for managing a cluster in the Genie system.")
public class Cluster extends CommonEntityFields {

    /**
     * Status of cluster - UP, OUT_OF_SERVICE or TERMINATED.
     */
    @Basic(optional = false)
    @Enumerated(EnumType.STRING)
    @ApiModelProperty(
            value = "The status of the cluster",
            required = true
    )
    @NotNull(message = "No cluster status entered and is required.")
    private ClusterStatus status;

    /**
     * The type of the cluster to use to figure out the job manager for this
     * cluster. eg: yarn, presto, mesos etc. The mapping JobManager will be
     * specified using the property:
     * netflix.genie.server.{clusterType}.JobManagerImpl
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "The type of the cluster to use to figure out the job manager for this"
                    + " cluster. e.g.: yarn, presto, mesos etc. The mapping to a JobManager will be"
                    + " specified using the property: com.netflix.genie.server.job.manager.<clusterType>.impl",
            required = true
    )
    @NotBlank(message = "No cluster type entered and is required.")
    private String clusterType;

    /**
     * Reference to all the configuration files needed for this cluster.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "All the configuration files needed for this cluster which will be downloaded pre-use"
    )
    private Set<String> configs;

    /**
     * Set of tags for a cluster.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "The tags associated with this cluster",
            required = true
    )
    private Set<String> tags;

    /**
     * Commands supported on this cluster - e.g. prodhive, testhive, etc.
     */
    @ManyToMany(fetch = FetchType.EAGER)
    @OrderColumn
    @JsonIgnore
    private List<Command> commands;

    /**
     * Default Constructor.
     */
    public Cluster() {
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
    public Cluster(
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
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateCluster() throws GeniePreconditionException {
        // Add the id to the tags
        if (this.tags == null) {
            this.tags = new HashSet<>();
        }
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
    public void setClusterType(String clusterType) {
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
        this.configs = configs;
    }

    /**
     * Gets the commands that this cluster supports.
     *
     * @return commands Not supposed to be exposed in request/response messages
     * hence marked transient.
     */
    public List<Command> getCommands() {
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
        this.tags = tags;
    }

    /**
     * Sets the commands for this cluster.
     *
     * @param commands The commands that this cluster supports
     */
    public void setCommands(final List<Command> commands) {
        //Clear references to this cluster in existing commands
        if (this.commands != null) {
            for (final Command command : this.commands) {
                if (command.getClusters() != null) {
                    command.getClusters().remove(this);
                }
            }
        }
        //set the commands for this command
        this.commands = commands;

        //Add the reference in the new commands
        if (this.commands != null) {
            for (final Command command : this.commands) {
                Set<Cluster> clusters = command.getClusters();
                if (clusters == null) {
                    clusters = new HashSet<>();
                    command.setClusters(clusters);
                }
                if (!clusters.contains(this)) {
                    clusters.add(this);
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
    public void addCommand(final Command command)
            throws GeniePreconditionException {
        if (command == null) {
            throw new GeniePreconditionException("No command entered unable to add.");
        }

        if (this.commands == null) {
            this.commands = new ArrayList<>();
        }
        this.commands.add(command);

        Set<Cluster> clusters = command.getClusters();
        if (clusters == null) {
            clusters = new HashSet<>();
            command.setClusters(clusters);
        }
        clusters.add(this);
    }

    /**
     * Remove an command from this command. Manages both sides of relationship.
     *
     * @param command The command to remove. Not null.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void removeCommand(final Command command)
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
            final List<Command> locCommands = new ArrayList<>();
            locCommands.addAll(this.commands);
            for (final Command command : locCommands) {
                this.removeCommand(command);
            }
        }
    }
}
