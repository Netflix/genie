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
package com.netflix.genie.web.jpa.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.ClusterRequest;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.controllers.DtoConverters;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.projections.ClusterCommandsProjection;
import com.netflix.genie.web.jpa.entities.v4.EntityDtoConverters;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.specifications.JpaClusterSpecs;
import com.netflix.genie.web.services.ClusterPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of the ClusterPersistenceService interface using JPA.
 *
 * @author amsharma
 * @author tgianos
 */
@Transactional(
    rollbackFor = {
        GenieException.class,
        GenieRuntimeException.class,
        ConstraintViolationException.class
    }
)
@Slf4j
public class JpaClusterPersistenceServiceImpl extends JpaBaseService implements ClusterPersistenceService {

    /**
     * Default constructor.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     */
    public JpaClusterPersistenceServiceImpl(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        super(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createCluster(
        @NotNull(message = "No cluster request entered. Unable to create.")
        @Valid final ClusterRequest request
    ) throws GenieException {
        log.debug("Called to create cluster with request {}", request);
        final ClusterEntity clusterEntity = this.createClusterEntity(request);
        try {
            this.getClusterRepository().save(clusterEntity);
        } catch (final DataIntegrityViolationException e) {
            throw new GenieConflictException(
                "A cluster with id " + clusterEntity.getUniqueId() + " already exists",
                e
            );
        }
        return clusterEntity.getUniqueId();
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
        return EntityDtoConverters.toV4ClusterDto(this.findCluster(id));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Cluster> getClusters(
        @Nullable final String name,
        @Nullable final Set<ClusterStatus> statuses,
        @Nullable final Set<String> tags,
        @Nullable final Instant minUpdateTime,
        @Nullable final Instant maxUpdateTime,
        final Pageable page
    ) {
        log.debug("called");

        final Set<TagEntity> tagEntities;
        // Find the tag entity references. If one doesn't exist return empty page as if the tag doesn't exist
        // no entities tied to that tag will exist either and today our search for tags is an AND
        if (tags != null) {
            tagEntities = this.getTagPersistenceService().getTags(tags);
            if (tagEntities.size() != tags.size()) {
                return new PageImpl<>(new ArrayList<>(), page, 0);
            }
        } else {
            tagEntities = null;
        }

        final Page<ClusterEntity> clusterEntities = this.getClusterRepository().findAll(
            JpaClusterSpecs.find(name, statuses, tagEntities, minUpdateTime, maxUpdateTime),
            page
        );

        return clusterEntities.map(EntityDtoConverters::toV4ClusterDto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Map<Cluster, String> findClustersAndCommandsForJob(
        @NotNull(message = "JobRequest object is null. Unable to continue.") final JobRequest jobRequest
    ) throws GenieException {
        log.debug("Called");

        final List<Criterion> clusterCriteria = Lists.newArrayList();
        for (final ClusterCriteria criteria : jobRequest.getClusterCriterias()) {
            clusterCriteria.add(DtoConverters.toV4Criterion(criteria));
        }

        final Criterion commandCriterion = DtoConverters.toV4Criterion(jobRequest.getCommandCriteria());

        return this.findClustersAndCommandsForJob(clusterCriteria, commandCriterion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Map<Cluster, String> findClustersAndCommandsForCriteria(
        @NotEmpty final List<@NotNull Criterion> clusterCriteria,
        @NotNull final Criterion commandCriterion
    ) throws GenieException {
        log.debug(
            "Attempting to find cluster and commands for cluster criteria {} and command criterion {}",
            clusterCriteria,
            commandCriterion
        );

        return this.findClustersAndCommandsForJob(clusterCriteria, commandCriterion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCluster(
        @NotBlank(message = "No cluster id entered. Unable to update.") final String id,
        @NotNull(message = "No cluster information entered. Unable to update.")
        @Valid final Cluster updateCluster
    ) throws GenieException {
        log.debug("Called with id {} and cluster {}", id, updateCluster);
        if (!this.getClusterRepository().existsByUniqueId(id)) {
            throw new GenieNotFoundException("No cluster exists with the given id. Unable to update.");
        }
        final String updateId = updateCluster.getId();
        if (!id.equals(updateId)) {
            throw new GenieBadRequestException("Cluster id inconsistent with id passed in.");
        }

        this.updateEntityWithDtoContents(this.findCluster(id), updateCluster);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void patchCluster(@NotBlank final String id, @NotNull final JsonPatch patch) throws GenieException {
        final ClusterEntity clusterEntity = this.findCluster(id);
        try {
            final Cluster clusterToPatch = EntityDtoConverters.toV4ClusterDto(clusterEntity);
            log.debug("Will patch cluster {}. Original state: {}", id, clusterToPatch);
            final JsonNode clusterNode = GenieObjectMapper.getMapper().valueToTree(clusterToPatch);
            final JsonNode postPatchNode = patch.apply(clusterNode);
            final Cluster patchedCluster = GenieObjectMapper.getMapper().treeToValue(postPatchNode, Cluster.class);
            log.debug("Finished patching cluster {}. New state: {}", id, patchedCluster);
            this.updateEntityWithDtoContents(clusterEntity, patchedCluster);
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
        for (final ClusterEntity clusterEntity : this.getClusterRepository().findAll()) {
            this.deleteCluster(clusterEntity.getUniqueId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCluster(
        @NotBlank(message = "No id entered unable to delete.") final String id
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
            }
        }
        this.getClusterRepository().delete(clusterEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConfigsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add configurations.") final String id,
        @NotEmpty(message = "No configuration files entered. Unable to add.") final Set<String> configs
    ) throws GenieException {
        log.debug("called");
        this.findCluster(id).getConfigs().addAll(this.createAndGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCluster(
        @NotBlank(message = "No cluster id sent. Cannot retrieve configurations.") final String id
    ) throws GenieException {
        log.debug("called");
        return this.findCluster(id).getConfigs().stream().map(FileEntity::getFile).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateConfigsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update configurations.") final String id,
        @NotEmpty(message = "No configs entered. Unable to update.") final Set<String> configs
    ) throws GenieException {
        log.debug("called with id {} and configs {}", id, configs);
        this.findCluster(id).setConfigs(this.createAndGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllConfigsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove configs.") final String id
    ) throws GenieException {
        this.findCluster(id).getConfigs().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add dependencies.") final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.") final Set<String> dependencies
    ) throws GenieException {
        this.findCluster(id).getDependencies().addAll(this.createAndGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to get dependencies.") final String id
    ) throws GenieException {
        return this.findCluster(id).getDependencies().stream().map(FileEntity::getFile).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update dependencies.") final String id,
        @NotNull(message = "No dependencies entered. Unable to update.") final Set<String> dependencies
    ) throws GenieException {
        this.findCluster(id).setDependencies(this.createAndGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllDependenciesForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove dependencies.") final String id
    ) throws GenieException {
        this.findCluster(id).getDependencies().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDependencyForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove dependency.") final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.") final String dependency
    ) throws GenieException {
        this.getFilePersistenceService().getFile(dependency).ifPresent(this.findCluster(id).getDependencies()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addTagsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add tags.") final String id,
        @NotEmpty(message = "No tags entered. Unable to add to tags.") final Set<String> tags
    ) throws GenieException {
        this.findCluster(id).getTags().addAll(this.createAndGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForCluster(
        @NotBlank(message = "No cluster id sent. Cannot retrieve tags.") final String id
    ) throws GenieException {
        return this.findCluster(id).getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTagsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update tags.") final String id,
        @NotEmpty(message = "No tags entered. Unable to update.") final Set<String> tags
    ) throws GenieException {
        this.findCluster(id).setTags(this.createAndGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllTagsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove tags.") final String id
    ) throws GenieException {
        this.findCluster(id).getTags().clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTagForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove tag.") final String id,
        @NotBlank(message = "No tag entered. Unable to remove.") final String tag
    ) throws GenieException {
        this.getTagPersistenceService().getTag(tag).ifPresent(this.findCluster(id).getTags()::remove);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to add commands.") final String id,
        @NotEmpty(message = "No command ids entered. Unable to add commands.") final List<String> commandIds
    ) throws GenieException {
        if (commandIds.size() != commandIds.stream().filter(this.getCommandRepository()::existsByUniqueId).count()) {
            throw new GeniePreconditionException("All commands need to exist to add to a cluster");
        }

        final ClusterEntity clusterEntity = this.findCluster(id);
        for (final String commandId : commandIds) {
            clusterEntity.addCommand(
                this.getCommandEntity(commandId).orElseThrow(
                    () -> new GenieNotFoundException("Couldn't find command with unique id " + commandId)
                )
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Command> getCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to get commands.") final String id,
        @Nullable final Set<CommandStatus> statuses
    ) throws GenieException {
        final Optional<ClusterCommandsProjection> commandsProjection
            = this.getClusterRepository().findByUniqueId(id, ClusterCommandsProjection.class);

        final List<CommandEntity> commandEntities = commandsProjection
            .orElseThrow(() -> new GenieNotFoundException("No cluster with id " + id + " exists"))
            .getCommands();
        if (statuses != null) {
            return commandEntities.stream()
                .filter(command -> statuses.contains(command.getStatus()))
                .map(EntityDtoConverters::toV4CommandDto)
                .collect(Collectors.toList());
        } else {
            return commandEntities.stream().map(EntityDtoConverters::toV4CommandDto).collect(Collectors.toList());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to update commands.") final String id,
        @NotNull(message = "No command ids entered. Unable to update commands.") final List<String> commandIds
    ) throws GenieException {
        if (commandIds.size() != commandIds.stream().filter(this.getCommandRepository()::existsByUniqueId).count()) {
            throw new GeniePreconditionException("All commands need to exist to add to a cluster");
        }
        final ClusterEntity clusterEntity = this.findCluster(id);
        final List<CommandEntity> commandEntities = new ArrayList<>();
        for (final String commandId : commandIds) {
            commandEntities.add(
                this.getCommandEntity(commandId).orElseThrow(
                    () -> new GenieNotFoundException("Couldn't find command with unique id " + commandId)
                )
            );
        }

        clusterEntity.setCommands(commandEntities);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllCommandsForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove commands.") final String id
    ) throws GenieException {
        this.findCluster(id).removeAllCommands();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeCommandForCluster(
        @NotBlank(message = "No cluster id entered. Unable to remove command.") final String id,
        @NotBlank(message = "No command id entered. Unable to remove command.") final String cmdId
    ) throws GenieException {
        this.findCluster(id)
            .removeCommand(
                this.getCommandEntity(cmdId).orElseThrow(
                    () -> new GenieNotFoundException("No command with id " + cmdId + " exists.")
                )
            );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long deleteTerminatedClusters() {
        return this.getClusterRepository().deleteByIdIn(
            this.getClusterRepository()
                .findTerminatedUnusedClusters()
                .stream()
                .map(Number::longValue)
                .collect(Collectors.toSet())
        );
    }

    private ClusterEntity createClusterEntity(final ClusterRequest request) {
        final ExecutionEnvironment resources = request.getResources();
        final ClusterMetadata metadata = request.getMetadata();

        final ClusterEntity entity = new ClusterEntity();
        this.setUniqueId(entity, request.getRequestedId().orElse(null));
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies, entity::setSetupFile);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setEntityClusterMetadata(entity, metadata);

        return entity;
    }

    private void updateEntityWithDtoContents(final ClusterEntity entity, final Cluster dto) {
        final ExecutionEnvironment resources = dto.getResources();
        final ClusterMetadata metadata = dto.getMetadata();

        // Save all the unowned entities first to avoid unintended flushes
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies, entity::setSetupFile);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setEntityClusterMetadata(entity, metadata);
    }

    private void setEntityClusterMetadata(final ClusterEntity entity, final ClusterMetadata metadata) {
        // NOTE: These are all called in case someone has changed it to set something to null. DO NOT use ifPresent
        entity.setName(metadata.getName());
        entity.setUser(metadata.getUser());
        entity.setVersion(metadata.getVersion());
        entity.setDescription(metadata.getDescription().orElse(null));
        entity.setStatus(metadata.getStatus());
        EntityDtoConverters.setJsonField(metadata.getMetadata().orElse(null), entity::setMetadata);
    }

    private Map<Cluster, String> findClustersAndCommandsForJob(
        final List<Criterion> clusterCriteria,
        final Criterion commandCriterion
    ) throws GenieServerException {
        final Map<Cluster, String> foundClusters = Maps.newHashMap();
        for (final Criterion clusterCriterion : clusterCriteria) {
            final List<Object[]> clusterCommands = this.getClusterRepository().resolveClustersAndCommands(
                clusterCriterion,
                commandCriterion
            );

            if (!clusterCommands.isEmpty()) {
                for (final Object[] ids : clusterCommands) {
                    if (ids.length != 2) {
                        throw new GenieServerException("Expected result length 2 but got " + ids.length);
                    }
                    final long clusterId;
                    if (ids[0] instanceof Number) {
                        clusterId = ((Number) ids[0]).longValue();
                    } else {
                        throw new GenieServerException("Expected number type but got " + ids[0].getClass().getName());
                    }
                    final String commandUniqueId;
                    if (ids[1] instanceof String) {
                        commandUniqueId = (String) ids[1];
                    } else {
                        throw new GenieServerException("Expected String type but got " + ids[1].getClass().getName());
                    }

                    final ClusterEntity clusterEntity = this.getClusterRepository().getOne(clusterId);
                    foundClusters.put(EntityDtoConverters.toV4ClusterDto(clusterEntity), commandUniqueId);
                }
                return foundClusters;
            }
        }

        //if we've gotten to here no clusters were found so return empty map
        return foundClusters;
    }

    /**
     * Helper method to find a cluster entity to save code.
     *
     * @param id The id of the cluster to find
     * @return The cluster entity if one exists
     * @throws GenieNotFoundException If the cluster doesn't exist
     */
    private ClusterEntity findCluster(final String id) throws GenieNotFoundException {
        return this.getClusterRepository()
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No cluster with id " + id + " exists."));
    }
}
