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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.NamedEntityGraphs;
import javax.persistence.NamedSubgraph;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Representation of the state of an Application.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Getter
@Setter
@ToString(
    callSuper = true,
    doNotUseGetters = true
)
@Entity
@Table(name = "applications")
@NamedEntityGraphs(
    {
        @NamedEntityGraph(
            name = ApplicationEntity.COMMANDS_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("commands")
            }
        ),
        @NamedEntityGraph(
            name = ApplicationEntity.COMMANDS_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "commands",
                    subgraph = "command-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "command-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("executable"),
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies"),
                        @NamedAttributeNode("tags"),
                        @NamedAttributeNode(
                            value = "clusterCriteria",
                            subgraph = "criteria-sub-graph"
                        )
                    }
                ),
                @NamedSubgraph(
                    name = "criteria-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("tags")
                    }
                )
            }
        ),
        @NamedEntityGraph(
            name = ApplicationEntity.DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("setupFile"),
                @NamedAttributeNode("configs"),
                @NamedAttributeNode("dependencies"),
                @NamedAttributeNode("tags")
            }
        )
    }
)
public class ApplicationEntity extends BaseEntity {

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load everything needed to access
     * an applications commands base fields.
     */
    public static final String COMMANDS_ENTITY_GRAPH = "Application.commands";

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load everything needed to access
     * an applications commands and create the command DTOs.
     */
    public static final String COMMANDS_DTO_ENTITY_GRAPH = "Application.commands.dto";

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load everything needed to construct an
     * Application DTO.
     */
    public static final String DTO_ENTITY_GRAPH = "Application.dto";

    private static final long serialVersionUID = -8780722054561507963L;

    @Basic
    @Column(name = "type")
    private String type;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "applications_configs",
        joinColumns = {
            @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> configs = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "applications_dependencies",
        joinColumns = {
            @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    private Set<FileEntity> dependencies = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "applications_tags",
        joinColumns = {
            @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<TagEntity> tags = new HashSet<>();

    @ManyToMany(mappedBy = "applications", fetch = FetchType.LAZY)
    @ToString.Exclude
    private Set<CommandEntity> commands = new HashSet<>();

    /**
     * Default constructor.
     */
    public ApplicationEntity() {
        super();
    }

    /**
     * Get the type of this application.
     *
     * @return The type as an Optional in case it's null
     */
    public Optional<String> getType() {
        return Optional.ofNullable(this.type);
    }

    /**
     * Set all the files associated as configuration files for this application.
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
     * Set all the files associated as dependency files for this application.
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
     * Set all the tags associated to this application.
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
     * Set all the commands associated with this application.
     *
     * @param commands The commands to set.
     */
    void setCommands(@Nullable final Set<CommandEntity> commands) {
        this.commands.clear();
        if (commands != null) {
            this.commands.addAll(commands);
        }
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
