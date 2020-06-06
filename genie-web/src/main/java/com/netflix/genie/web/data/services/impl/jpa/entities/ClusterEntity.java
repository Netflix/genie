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
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.NamedEntityGraphs;
import javax.persistence.Table;
import java.util.HashSet;
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
@ToString(
    callSuper = true,
    doNotUseGetters = true
)
@Entity
@Table(name = "clusters")
@NamedEntityGraphs(
    {
        @NamedEntityGraph(
            name = ClusterEntity.DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("setupFile"),
                @NamedAttributeNode("configs"),
                @NamedAttributeNode("dependencies"),
                @NamedAttributeNode("tags")
            }
        )
    }
)
public class ClusterEntity extends BaseEntity {

    /**
     * The name of the {@link javax.persistence.EntityGraph} which will eagerly load everything needed to construct a
     * Cluster DTO.
     */
    public static final String DTO_ENTITY_GRAPH = "Cluster.dto";

    private static final long serialVersionUID = -5674870110962005872L;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "clusters_configs",
        joinColumns = {
            @JoinColumn(name = "cluster_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> configs = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "clusters_dependencies",
        joinColumns = {
            @JoinColumn(name = "cluster_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> dependencies = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "clusters_tags",
        joinColumns = {
            @JoinColumn(name = "cluster_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @ToString.Exclude
    private Set<TagEntity> tags = new HashSet<>();

    /**
     * Default Constructor.
     */
    public ClusterEntity() {
        super();
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
