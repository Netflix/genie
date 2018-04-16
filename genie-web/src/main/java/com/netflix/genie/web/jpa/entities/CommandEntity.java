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
package com.netflix.genie.web.jpa.entities;

import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;
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
@ToString(callSuper = true, of = {"status", "executable", "checkDelay", "memory"})
@Entity
@Table(name = "commands")
public class CommandEntity extends BaseEntity {

    private static final long serialVersionUID = -8058995173025433517L;

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @NotNull(message = "No command status entered and is required.")
    private CommandStatus status;

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
    private List<ApplicationEntity> applications = new ArrayList<>();

    @ManyToMany(mappedBy = "commands", fetch = FetchType.LAZY)
    private Set<ClusterEntity> clusters = new HashSet<>();

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
     * Sets the applications for this command.
     *
     * @param applications The application that this command uses
     * @throws GeniePreconditionException if the list of applications contains duplicates
     */
    public void setApplications(@Nullable final List<ApplicationEntity> applications)
        throws GeniePreconditionException {
        if (applications != null
            && applications.stream().map(ApplicationEntity::getUniqueId).distinct().count() != applications.size()) {
            throw new GeniePreconditionException("List of applications to set cannot contain duplicates");
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
     * @throws GeniePreconditionException If the application is a duplicate of an existing application
     */
    public void addApplication(@NotNull final ApplicationEntity application) throws GeniePreconditionException {
        if (this.applications.contains(application)) {
            throw new GeniePreconditionException(
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
     * Set the clusters this command is available on.
     *
     * @param clusters the clusters
     */
    protected void setClusters(@Nullable final Set<ClusterEntity> clusters) {
        this.clusters.clear();
        if (clusters != null) {
            this.clusters.addAll(clusters);
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
