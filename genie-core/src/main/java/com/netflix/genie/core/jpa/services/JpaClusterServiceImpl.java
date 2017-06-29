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
package com.netflix.genie.core.jpa.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.specifications.JpaClusterSpecs;
import com.netflix.genie.core.services.ClusterService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Implementation of the ClusterService interface using JPA.
 *
 * @author amsharma
 * @author tgianos
 */
@Transactional(
    rollbackFor = {
        GenieException.class,
        ConstraintViolationException.class
    }
)
@Slf4j
public class JpaClusterServiceImpl implements ClusterService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final JpaClusterRepository clusterRepo;
    private final JpaCommandRepository commandRepo;

    /**
     * Default constructor - initialize all required dependencies.
     *
     * @param clusterRepo The cluster repository to use.
     * @param commandRepo The command repository to use.
     */
    public JpaClusterServiceImpl(
        final JpaClusterRepository clusterRepo,
        final JpaCommandRepository commandRepo
    ) {
        this.clusterRepo = clusterRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createCluster(
        @NotNull(message = "No cluster entered. Unable to create.")
        @Valid
        final Cluster cluster
    ) throws GenieException {
        log.debug("Called to create cluster {}", cluster);
        final Optional<String> clusterId = cluster.getId();
        if (clusterId.isPresent() && this.clusterRepo.exists(clusterId.get())) {
            throw new GenieConflictException("A cluster with id " + clusterId.get() + " already exists");
        }

        final ClusterEntity clusterEntity = new ClusterEntity();
        clusterEntity.setId(cluster.getId().orElse(UUID.randomUUID().toString()));
        this.updateAndSaveClusterEntity(clusterEntity, cluster);
        return clusterEntity.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Cluster getCluster(
        @NotBlank(message = "No id entered. Unable to get.") final String id
    ) throws GenieException {
        log.debug("Called with id {}", id);
        return this.findCluster(id).getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Cluster> getClusters(
        final String name,
        final Set<ClusterStatus> statuses,
        final Set<String> tags,
        final Date minUpdateTime,
        final Date maxUpdateTime,
        final Pageable page
    ) {
        log.debug("called");

        @SuppressWarnings("unchecked")
        final Page<ClusterEntity> clusterEntities = this.clusterRepo.findAll(
            JpaClusterSpecs.find(name, statuses, tags, minUpdateTime, maxUpdateTime),
            page
        );

        return clusterEntities.map(ClusterEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Cluster> chooseClusterForJobRequest(
        @NotNull(message = "JobRequest object is null. Unable to continue.")
        final JobRequest jobRequest
    ) throws GenieException {
        log.debug("Called");

        final List<ClusterCriteria> clusterCriterias = jobRequest.getClusterCriterias();
        final Set<String> commandCriteria = jobRequest.getCommandCriteria();

        for (final ClusterCriteria clusterCriteria : clusterCriterias) {
            @SuppressWarnings("unchecked")
            final List<ClusterEntity> clusterEntities = this.clusterRepo.findAll(
                JpaClusterSpecs.findByClusterAndCommandCriteria(
                    clusterCriteria,
                    commandCriteria
                )
            );

            if (!clusterEntities.isEmpty()) {
                return clusterEntities
                    .stream()
                    .map(ClusterEntity::getDTO)
                    .collect(Collectors.toList());
            }
        }

        //if we've gotten to here no clusters were found so return empty list
        return new ArrayList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCluster(
        @NotBlank(message = "No cluster id entered. Unable to update.")
        final String id,
        @NotNull(message = "No cluster information entered. Unable to update.")
        @Valid
        final com.netflix.genie.common.dto.Cluster updateCluster
    ) throws GenieException {
        log.debug("Called with id {} and cluster {}", id, updateCluster);
        if (!this.clusterRepo.exists(id)) {
            throw new GenieNotFoundException("No cluster exists with the given id. Unable to update.");
        }
        final Optional<String> updateId = updateCluster.getId();
        if (updateId.isPresent() && !id.equals(updateId.get())) {
            throw new GenieBadRequestException("Cluster id inconsistent with id passed in.");
        }

        //TODO: Move update of common fields to super classes
        this.updateAndSaveClusterEntity(this.clusterRepo.findOne(id), updateCluster);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void patchCluster(@NotBlank final String id, @NotNull final JsonPatch patch) throws GenieException {
        final ClusterEntity clusterEntity = this.findCluster(id);
        try {
            final Cluster clusterToPatch = clusterEntity.getDTO();
            log.debug("Will patch cluster {}. Original state: {}", id, clusterToPatch);
            final JsonNode clusterNode = this.mapper.readTree(clusterToPatch.toString());
            final JsonNode postPatchNode = patch.apply(clusterNode);
            final Cluster patchedCluster = this.mapper.treeToValue(postPatchNode, Cluster.class);
            log.debug("Finished patching cluster {}. New state: {}", id, patchedCluster);
            this.updateAndSaveClusterEntity(clusterEntity, patchedCluster);
        } catch (final JsonPatchException | IOException e) {
            log.error("Unable to patch cluster {} with patch {} due to exception.", id, patch, e);
            throw new GenieServerException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllClusters() throws GenieException {
        log.debug("Called to delete all clusters");
        for (final ClusterEntity clusterEntity : this.clusterRepo.findAll()) {
            this.deleteCluster(clusterEntity.getId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCluster(
        @NotBlank(message = "No id entered unable to delete.")
        final String id
    ) throws GenieException {
        log.debug("Called");
        final ClusterEntity clusterEntity = this.findCluster(id);
        final List<CommandEntity> commandEntities = clusterEntity.getCommands();
        if (commandEntities != null) {
            for (final CommandEntity commandEntity : commandEntities) {
                final Set<ClusterEntity> clusterEntities = commandEntity.getClusters();
                if (clusterEntities != null) {
                    clusterEntities.remove(clusterEntity);
                }
                this.commandRepo.save(commandEntity);
            }
        }
        this.clusterRepo.delete(clusterEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add configurations.")
        final String id,
        @NotEmpty(message = "No configuration files entered. Unable to add.")
        final Set<String> configs
    ) throws GenieException {
        log.debug("called");
        this.findCluster(id).getConfigs().addAll(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCluster(
        @NotBlank(message = "No cluster id sent. Cannot retrieve configurations.")
        final String id
    ) throws GenieException {
        log.debug("called");
        return this.findCluster(id).getConfigs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update configurations.")
        final String id,
        @NotEmpty(message = "No configs entered. Unable to update.")
        final Set<String> configs
    ) throws GenieException {
        log.debug("called with id {} and configs {}", id, configs);
        this.findCluster(id).setConfigs(configs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove configs.")
        final String id
    ) throws GenieException {
        this.findCluster(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add dependencies.")
        final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.")
        final Set<String> dependencies
    ) throws GenieException {
        this.findCluster(id).getDependencies().addAll(dependencies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to get dependencies.")
        final String id
    ) throws GenieException {
        return this.findCluster(id).getDependencies();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update dependencies.")
        final String id,
        @NotNull(message = "No dependencies entered. Unable to update.")
        final Set<String> dependencies
    ) throws GenieException {
        this.findCluster(id).setDependencies(dependencies);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove dependencies.")
        final String id
    ) throws GenieException {
        this.findCluster(id).getDependencies().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDependencyForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove dependency.")
        final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.")
        final String dependency
    ) throws GenieException {
        this.findCluster(id).getDependencies().remove(dependency);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add tags.")
        final String id,
        @NotEmpty(message = "No tags entered. Unable to add to tags.")
        final Set<String> tags
    ) throws GenieException {
        final ClusterEntity cluster = this.findCluster(id);
        final Set<String> clusterTags = cluster.getTags();
        clusterTags.addAll(tags);
        cluster.setTags(clusterTags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForCluster(
        @NotBlank(message = "No cluster id sent. Cannot retrieve tags.")
        final String id
    ) throws GenieException {
        return this.findCluster(id).getTags();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update tags.")
        final String id,
        @NotEmpty(message = "No tags entered. Unable to update.")
        final Set<String> tags
    ) throws GenieException {
        this.findCluster(id).setTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove tags.")
        final String id
    ) throws GenieException {
        this.findCluster(id).setTags(Sets.newHashSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove tag.")
        final String id,
        @NotBlank(message = "No tag entered. Unable to remove.")
        final String tag
    ) throws GenieException {
        final ClusterEntity cluster = this.findCluster(id);
        final Set<String> tags = cluster.getTags();
        tags.remove(tag);
        cluster.setTags(tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add commands.")
        final String id,
        @NotEmpty(message = "No command ids entered. Unable to add commands.")
        final List<String> commandIds
    ) throws GenieException {
        if (commandIds.size() != commandIds.stream().filter(this.commandRepo::exists).count()) {
            throw new GeniePreconditionException("All commands need to exist to add to a cluster");
        }

        final ClusterEntity clusterEntity = this.findCluster(id);
        for (final String commandId : commandIds) {
            clusterEntity.addCommand(this.commandRepo.findOne(commandId));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Command> getCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to get commands.")
        final String id,
        final Set<CommandStatus> statuses
    ) throws GenieException {
        final ClusterEntity clusterEntity = this.findCluster(id);
        final List<CommandEntity> commandEntities = clusterEntity.getCommands();
        if (statuses != null) {
            return commandEntities.stream()
                .filter(command -> statuses.contains(command.getStatus()))
                .map(CommandEntity::getDTO)
                .collect(Collectors.toList());
        } else {
            return commandEntities.stream().map(CommandEntity::getDTO).collect(Collectors.toList());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update commands.")
        final String id,
        @NotNull(message = "No command ids entered. Unable to update commands.")
        final List<String> commandIds
    ) throws GenieException {
        if (commandIds.size() != commandIds.stream().filter(this.commandRepo::exists).count()) {
            throw new GeniePreconditionException("All commands need to exist to add to a cluster");
        }
        final ClusterEntity clusterEntity = this.findCluster(id);
        final List<CommandEntity> commandEntities = new ArrayList<>();
        commandIds.forEach(commandId -> commandEntities.add(this.commandRepo.findOne(commandId)));

        clusterEntity.setCommands(commandEntities);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove commands.")
        final String id
    ) throws GenieException {
        this.findCluster(id).removeAllCommands();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeCommandForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove command.")
        final String id,
        @NotBlank(message = "No command id entered. Unable to remove command.")
        final String cmdId
    ) throws GenieException {
        final ClusterEntity clusterEntity = this.findCluster(id);
        final CommandEntity commandEntity = this.commandRepo.findOne(cmdId);
        if (commandEntity != null) {
            clusterEntity.removeCommand(commandEntity);
        } else {
            throw new GenieNotFoundException("No command with id " + cmdId + " exists.");
        }
    }

    /**
     * Helper method to find a cluster entity to save code.
     *
     * @param id The id of the cluster to find
     * @return The cluster entity if one exists
     * @throws GenieNotFoundException If the cluster doesn't exist
     */
    private ClusterEntity findCluster(final String id) throws GenieNotFoundException {
        final ClusterEntity clusterEntity = this.clusterRepo.findOne(id);
        if (clusterEntity != null) {
            return clusterEntity;
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    private void updateAndSaveClusterEntity(final ClusterEntity clusterEntity, final Cluster updateCluster) {
        clusterEntity.setName(updateCluster.getName());
        clusterEntity.setUser(updateCluster.getUser());
        clusterEntity.setVersion(updateCluster.getVersion());
        final Optional<String> description = updateCluster.getDescription();
        clusterEntity.setDescription(description.orElse(null));
        clusterEntity.setStatus(updateCluster.getStatus());
        clusterEntity.setConfigs(updateCluster.getConfigs());
        clusterEntity.setDependencies(updateCluster.getDependencies());
        clusterEntity.setTags(updateCluster.getTags());
        final Optional<String> setupFile = updateCluster.getSetupFile();
        clusterEntity.setSetupFile(setupFile.orElse(null));

        this.clusterRepo.save(clusterEntity);
    }
}
