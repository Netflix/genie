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

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieException;
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
import javax.persistence.ManyToMany;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Representation of the state of Application Configuration object.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Getter
@Setter
@Entity
@Table(name = "applications")
public class ApplicationEntity extends SetupFileEntity {

    private static final long serialVersionUID = -8780722054561507963L;

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    @NotNull(message = "No application status entered and is required.")
    private ApplicationStatus status;

    @Basic
    @Column(name = "type")
    private String type;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "application_configs",
        joinColumns = @JoinColumn(name = "application_id", referencedColumnName = "id")
    )
    @Column(name = "config", nullable = false, length = 2048)
    private Set<String> configs = new HashSet<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "application_dependencies",
        joinColumns = @JoinColumn(name = "application_id", referencedColumnName = "id")
    )
    @Column(name = "dependency", nullable = false, length = 2048)
    private Set<String> dependencies = new HashSet<>();

    @ManyToMany(mappedBy = "applications", fetch = FetchType.LAZY)
    private Set<CommandEntity> commands = new HashSet<>();

    /**
     * Default constructor.
     */
    public ApplicationEntity() {
        super();
    }

    /**
     * Check to make sure everything is OK before persisting.
     *
     * @throws GenieException If any preconditions aren't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateApplication() throws GenieException {
        this.setTags(this.getFinalTags());
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
     * Sets the configurations for this application.
     *
     * @param configs The configuration files that this application needs
     */
    public void setConfigs(final Set<String> configs) {
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Sets the dependencies needed for this application.
     *
     * @param dependencies All dependencies needed for execution of this application
     */
    public void setDependencies(final Set<String> dependencies) {
        this.dependencies.clear();
        if (dependencies != null) {
            this.dependencies.addAll(dependencies);
        }
    }

    /**
     * Set all the commands associated with this application.
     *
     * @param commands The commands to set.
     */
    protected void setCommands(final Set<CommandEntity> commands) {
        this.commands.clear();
        if (commands != null) {
            this.commands.addAll(commands);
        }
    }

    /**
     * Get a DTO from this entity.
     *
     * @return DTO of this entity.
     */
    public Application getDTO() {
        final Application.Builder builder = new Application
            .Builder(this.getName(), this.getUser(), this.getVersion(), this.status)
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .withTags(this.getTags())
            .withConfigs(this.configs)
            .withDependencies(this.dependencies)
            .withType(this.type);

        this.getDescription().ifPresent(builder::withDescription);
        this.getSetupFile().ifPresent(builder::withSetupFile);

        return builder.build();
    }
}
