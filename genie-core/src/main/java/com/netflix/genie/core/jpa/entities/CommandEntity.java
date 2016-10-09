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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
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
import javax.persistence.OrderColumn;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
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
@Entity
@Table(name = "commands")
public class CommandEntity extends SetupFileEntity {

    private static final long serialVersionUID = -8058995173025433517L;

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @NotNull(message = "No command status entered and is required.")
    private CommandStatus status;

    @Basic(optional = false)
    @Column(name = "executable", nullable = false)
    @NotBlank(message = "No executable entered for command and is required.")
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String executable;

    @Basic(optional = false)
    @Column(name = "check_delay", nullable = false)
    @Min(1)
    private long checkDelay = Command.DEFAULT_CHECK_DELAY;

    @Basic
    @Column(name = "memory")
    @Min(1)
    private Integer memory;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "command_configs",
        joinColumns = @JoinColumn(name = "command_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 1024)
    private Set<String> configs = new HashSet<>();

    // TODO: Make lazy?
    @ManyToMany(fetch = FetchType.EAGER)
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
     * Get the default memory for a job using this command.
     *
     * @return Optional of Integer as it could be null
     */
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
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
     * Sets the applications for this command.
     *
     * @param applications The application that this command uses
     * @throws GeniePreconditionException if the list of applications contains duplicates
     */
    public void setApplications(final List<ApplicationEntity> applications) throws GeniePreconditionException {
        if (applications != null
            && applications.stream().map(ApplicationEntity::getId).distinct().count() != applications.size()) {
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
        if (
            this.applications
                .stream()
                .map(ApplicationEntity::getId)
                .filter(id -> id.equals(application.getId()))
                .count() != 0
            ) {
            throw new GeniePreconditionException("An application with id " + application.getId() + " is already added");
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
    protected void setClusters(final Set<ClusterEntity> clusters) {
        this.clusters.clear();
        if (clusters != null) {
            this.clusters.addAll(clusters);
        }
    }

    /**
     * Get a dto based on the information in this entity.
     *
     * @return The dto
     */
    public Command getDTO() {
        final Command.Builder builder = new Command.Builder(
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
            .withTags(this.getTags())
            .withConfigs(this.configs)
            .withMemory(this.memory);

        this.getDescription().ifPresent(builder::withDescription);
        this.getSetupFile().ifPresent(builder::withSetupFile);

        return builder.build();
    }
}
