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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.web.data.services.impl.jpa.converters.JsonAttributeConverter;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.Lob;
import javax.persistence.ManyToMany;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.NamedEntityGraphs;
import javax.persistence.NamedSubgraph;
import javax.persistence.OrderColumn;
import javax.persistence.Table;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Representation of the state of the Command Object.
 *
 * @author amsharma
 * @author tgianos
 */
@Getter
@Setter
@ToString(
    callSuper = true,
    doNotUseGetters = true
)
@Entity
@Table(name = "commands")
@NamedEntityGraphs(
    {
        @NamedEntityGraph(
            name = CommandEntity.APPLICATIONS_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "applications",
                    subgraph = "application-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "application-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies"),
                        @NamedAttributeNode("tags")
                    }
                )
            }
        ),
        @NamedEntityGraph(
            name = CommandEntity.APPLICATIONS_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "applications",
                    subgraph = "application-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "application-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies"),
                        @NamedAttributeNode("tags")
                    }
                )
            }
        ),
        @NamedEntityGraph(
            name = CommandEntity.CLUSTER_CRITERIA_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "clusterCriteria",
                    subgraph = "criteria-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "criteria-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("tags")
                    }
                )
            }
        ),
        @NamedEntityGraph(
            name = CommandEntity.DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("executable"),
                @NamedAttributeNode("launcherExt"),
                @NamedAttributeNode("setupFile"),
                @NamedAttributeNode("configs"),
                @NamedAttributeNode("dependencies"),
                @NamedAttributeNode("tags"),
                @NamedAttributeNode(
                    value = "clusterCriteria",
                    subgraph = "criteria-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "criteria-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("tags")
                    }
                )
            }
        )
    }
)
public class CommandEntity extends BaseEntity {

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load the command base fields and
     * its associated applications base fields.
     */
    public static final String APPLICATIONS_ENTITY_GRAPH = "Command.applications";

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load the command base fields and
     * its associated applications dto fields.
     */
    public static final String APPLICATIONS_DTO_ENTITY_GRAPH = "Command.applications.dto";

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load the command base fields and
     * its associated cluster criteria.
     */
    public static final String CLUSTER_CRITERIA_ENTITY_GRAPH = "Command.clusterCriteria";

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load everything needed to construct a
     * Command DTO.
     */
    public static final String DTO_ENTITY_GRAPH = "Command.DTO";

    private static final long serialVersionUID = -8058995173025433517L;
    private static final int HIGHEST_CRITERION_PRIORITY = 0;

    @ElementCollection
    @CollectionTable(
        name = "command_executable_arguments",
        joinColumns = {
            @JoinColumn(name = "command_id", nullable = false)
        }
    )
    @Column(name = "argument", length = 1024, nullable = false)
    @OrderColumn(name = "argument_order", nullable = false)
    @NotEmpty(message = "No executable arguments entered. At least one is required.")
    private List<@NotBlank @Size(max = 1024) String> executable = new ArrayList<>();

    @Basic(optional = false)
    @Column(name = "check_delay", nullable = false)
    @Min(1)
    private long checkDelay = Command.DEFAULT_CHECK_DELAY;

    @Basic
    @Column(name = "memory")
    @Min(1)
    private Integer memory;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "launcher_ext", columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode launcherExt;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "commands_configs",
        joinColumns = {
            @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> configs = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "commands_dependencies",
        joinColumns = {
            @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> dependencies = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "commands_tags",
        joinColumns = {
            @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<TagEntity> tags = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "commands_applications",
        joinColumns = {
            @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @OrderColumn(name = "application_order", nullable = false)
    @ToString.Exclude
    private List<ApplicationEntity> applications = new ArrayList<>();

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(
        name = "commands_cluster_criteria",
        joinColumns = {
            @JoinColumn(name = "command_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "criterion_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @OrderColumn(name = "priority_order", nullable = false)
    @ToString.Exclude
    private List<CriterionEntity> clusterCriteria = new ArrayList<>();

    /**
     * Default Constructor.
     */
    public CommandEntity() {
        super();
    }

    /**
     * Set the executable and any default arguments for this command.
     *
     * @param executable The executable and default arguments which can't be blank and there must be at least one
     */
    public void setExecutable(@NotEmpty final List<@NotBlank @Size(max = 1024) String> executable) {
        this.executable.clear();
        this.executable.addAll(executable);
    }

    /**
     * Set all the files associated as configuration files for this cluster.
     *
     * @param configs The configuration files to set
     */
    public void setConfigs(@Nullable final Set<FileEntity> configs) {
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Set all the files associated as dependency files for this cluster.
     *
     * @param dependencies The dependency files to set
     */
    public void setDependencies(@Nullable final Set<FileEntity> dependencies) {
        this.dependencies.clear();
        if (dependencies != null) {
            this.dependencies.addAll(dependencies);
        }
    }

    /**
     * Set all the tags associated to this cluster.
     *
     * @param tags The dependency tags to set
     */
    public void setTags(@Nullable final Set<TagEntity> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Get the default memory for a job using this command.
     *
     * @return Optional of Integer as it could be null
     */
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
    }

    /**
     * Get any metadata associated with this command pertaining to specifying details for various agent launchers.
     *
     * @return The metadata or {@link Optional#empty()} if there isn't any
     */
    public Optional<JsonNode> getLauncherExt() {
        return Optional.ofNullable(this.launcherExt);
    }

    /**
     * Set any metadata pertaining to additional instructions for various launchers if this command is used.
     *
     * @param launcherExt The metadata
     */
    public void setLauncherExt(@Nullable final JsonNode launcherExt) {
        this.launcherExt = launcherExt;
    }

    /**
     * Sets the applications for this command.
     *
     * @param applications The application that this command uses
     * @throws PreconditionFailedException if the list of applications contains duplicates
     */
    public void setApplications(
        @Nullable final List<ApplicationEntity> applications
    ) throws PreconditionFailedException {
        if (applications != null
            && applications.stream().map(ApplicationEntity::getUniqueId).distinct().count() != applications.size()) {
            throw new PreconditionFailedException("List of applications to set cannot contain duplicates");
        }

        //Clear references to this command in existing applications
        for (final ApplicationEntity application : this.applications) {
            application.getCommands().remove(this);
        }
        this.applications.clear();

        if (applications != null) {
            //set the application for this command
            this.applications.addAll(applications);

            //Add the reverse reference in the new applications
            for (final ApplicationEntity application : this.applications) {
                application.getCommands().add(this);
            }
        }
    }

    /**
     * Append an application to the list of applications this command uses.
     *
     * @param application The application to add. Not null.
     * @throws PreconditionFailedException If the application is a duplicate of an existing application
     */
    public void addApplication(@NotNull final ApplicationEntity application) throws PreconditionFailedException {
        if (this.applications.contains(application)) {
            throw new PreconditionFailedException(
                "An application with id " + application.getUniqueId() + " is already added"
            );
        }

        this.applications.add(application);
        application.getCommands().add(this);
    }

    /**
     * Remove an application from this command. Manages both sides of relationship.
     *
     * @param application The application to remove. Not null.
     */
    public void removeApplication(@NotNull final ApplicationEntity application) {
        this.applications.remove(application);
        application.getCommands().remove(this);
    }

    /**
     * Set the criteria, in priority order, that this command has for determining which available clusters to use
     * for a job.
     *
     * @param clusterCriteria The cluster criteria
     */
    public void setClusterCriteria(@Nullable final List<CriterionEntity> clusterCriteria) {
        this.clusterCriteria.clear();
        if (clusterCriteria != null) {
            this.clusterCriteria.addAll(clusterCriteria);
        }
    }

    /**
     * Add a new cluster criterion as the lowest priority criterion to evaluate for this command.
     *
     * @param criterion The {@link CriterionEntity} to add
     */
    public void addClusterCriterion(final CriterionEntity criterion) {
        this.clusterCriteria.add(criterion);
    }

    /**
     * Add a new cluster criterion with the given priority.
     *
     * @param criterion The new criterion to add
     * @param priority  The priority with which this criterion should be considered. {@literal 0} would be the highest
     *                  priority and anything greater than the current size of the existing criteria will just be added
     *                  to the end of the list. If a priority of {@code < 0} is passed in the criterion is added
     *                  as the highest priority ({@literal 0}).
     */
    public void addClusterCriterion(final CriterionEntity criterion, final int priority) {
        if (priority <= HIGHEST_CRITERION_PRIORITY) {
            this.clusterCriteria.add(HIGHEST_CRITERION_PRIORITY, criterion);
        } else if (priority >= this.clusterCriteria.size()) {
            this.clusterCriteria.add(criterion);
        } else {
            this.clusterCriteria.add(priority, criterion);
        }
    }

    /**
     * Remove the criterion with the given priority from the list of available criterion for this command.
     *
     * @param priority The priority of the criterion to remove.
     * @return The {@link CriterionEntity} which was removed by this operation
     * @throws IllegalArgumentException If this value is {@code < 0} or {@code > {@link List#size()}} as it becomes
     *                                  unclear what the user wants to do
     */
    public CriterionEntity removeClusterCriterion(final int priority) throws IllegalArgumentException {
        if (priority < HIGHEST_CRITERION_PRIORITY || priority >= this.clusterCriteria.size()) {
            throw new IllegalArgumentException(
                "The priority of the cluster criterion to remove must be "
                    + HIGHEST_CRITERION_PRIORITY
                    + "<= priority < "
                    + this.clusterCriteria.size()
                    + " for this command currently."
            );
        }
        return this.clusterCriteria.remove(priority);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
