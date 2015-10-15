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

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.ClusterRepository;
import com.netflix.genie.core.jpa.repositories.ClusterSpecs;
import com.netflix.genie.core.jpa.repositories.CommandRepository;
import com.netflix.genie.core.jpa.repositories.JobRepository;
import com.netflix.genie.core.services.ClusterService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Implementation of the ClusterService interface using JPA.
 *
 * @author amsharma
 * @author tgianos
 */
@Service
@Transactional(
        rollbackFor = {
                GenieException.class,
                ConstraintViolationException.class
        }
)
public class ClusterServiceJPAImpl implements ClusterService {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterServiceJPAImpl.class);
    private static final char CRITERIA_DELIMITER = ',';
    private final ClusterRepository clusterRepo;
    private final CommandRepository commandRepo;
    private final JobRepository jobRepo;

    /**
     * Default constructor - initialize all required dependencies.
     *
     * @param clusterRepo The cluster repository to use.
     * @param commandRepo The command repository to use.
     * @param jobRepo     The job repository to use.
     */
    @Autowired
    public ClusterServiceJPAImpl(
            final ClusterRepository clusterRepo,
            final CommandRepository commandRepo,
            final JobRepository jobRepo
    ) {
        this.clusterRepo = clusterRepo;
        this.commandRepo = commandRepo;
        this.jobRepo = jobRepo;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to create cluster " + cluster.toString());
        }
        if (StringUtils.isNotBlank(cluster.getId()) && this.clusterRepo.exists(cluster.getId())) {
            throw new GenieConflictException("A cluster with id " + cluster.getId() + " already exists");
        }

        final ClusterEntity clusterEntity
                = new ClusterEntity();
        clusterEntity.setId(StringUtils.isBlank(cluster.getId()) ? UUID.randomUUID().toString() : cluster.getId());
        clusterEntity.setName(cluster.getName());
        clusterEntity.setUser(cluster.getUser());
        clusterEntity.setVersion(cluster.getVersion());
        clusterEntity.setDescription(cluster.getDescription());
        clusterEntity.setClusterType(cluster.getClusterType());
        clusterEntity.setStatus(cluster.getStatus());
        clusterEntity.setConfigs(cluster.getConfigs());
        clusterEntity.setTags(cluster.getTags());

        return this.clusterRepo.save(clusterEntity).getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Cluster getCluster(
            @NotBlank(message = "No id entered. Unable to get.")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
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
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final Pageable page
    ) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }

        @SuppressWarnings("unchecked")
        final Page<ClusterEntity> clusterEntities
                = this.clusterRepo.findAll(ClusterSpecs.find(name, statuses, tags, minUpdateTime, maxUpdateTime), page);

        return clusterEntities.map(ClusterEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public List<Cluster> chooseClusterForJob(
            @NotBlank(message = "No job id entered. Unable to continue.")
            final String jobId
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called");
        }
        final JobEntity jobEntity = this.jobRepo.findOne(jobId);
        if (jobEntity == null) {
            throw new GenieNotFoundException("No job with id " + jobId + " exists. Unable to continue."
            );
        }

        final List<ClusterCriteria> clusterCriterias = jobEntity.getClusterCriterias();
        final Set<String> commandCriteria = jobEntity.getCommandCriteria();

        for (final ClusterCriteria clusterCriteria : clusterCriterias) {
            @SuppressWarnings("unchecked")
            final List<ClusterEntity> clusterEntities = this.clusterRepo.findAll(
                    ClusterSpecs.findByClusterAndCommandCriteria(
                            clusterCriteria,
                            commandCriteria
                    )
            );

            if (!clusterEntities.isEmpty()) {
                // Add the successfully criteria to the job object in string form.
                jobEntity.setChosenClusterCriteriaString(
                        StringUtils.join(
                                clusterCriteria.getTags(),
                                CRITERIA_DELIMITER
                        )
                );
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and cluster " + updateCluster);
        }
        if (!this.clusterRepo.exists(id)) {
            throw new GenieNotFoundException("No cluster exists with the given id. Unable to update.");
        }
        if (!id.equals(updateCluster.getId())) {
            throw new GenieBadRequestException("Cluster id inconsistent with id passed in.");
        }

        //TODO: Move update of common fields to super classes
        final ClusterEntity clusterEntity = this.clusterRepo.findOne(id);
        clusterEntity.setName(updateCluster.getName());
        clusterEntity.setUser(updateCluster.getUser());
        clusterEntity.setVersion(updateCluster.getVersion());
        clusterEntity.setDescription(updateCluster.getDescription());
        clusterEntity.setClusterType(updateCluster.getClusterType());
        clusterEntity.setStatus(updateCluster.getStatus());
        clusterEntity.setConfigs(updateCluster.getConfigs());
        clusterEntity.setTags(updateCluster.getTags());

        this.clusterRepo.save(clusterEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllClusters() throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to delete all clusters");
        }
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called");
        }
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("called with id " + id + " and configs " + configs);
        }
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
    public void addTagsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to add to tags.")
            final Set<String> tags
    ) throws GenieException {
        this.findCluster(id).getTags().addAll(tags);
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
        this.findCluster(id).getTags().clear();
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
        this.findCluster(id).getTags().remove(tag);
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
        final ClusterEntity clusterEntity = this.findCluster(id);
        for (final String commandId : commandIds) {
            final CommandEntity cmd = this.commandRepo.findOne(commandId);
            if (cmd != null) {
                clusterEntity.addCommand(cmd);
            } else {
                throw new GenieNotFoundException("No command with id " + commandId + " exists.");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<com.netflix.genie.common.dto.Command> getCommandsForCluster(
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
    public void updateCommandsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to update commands.")
            final String id,
            @NotNull(message = "No command ids entered. Unable to update commands.")
            final List<String> commandIds
    ) throws GenieException {
        final ClusterEntity clusterEntity = this.findCluster(id);
        final List<CommandEntity> commandEntities = new ArrayList<>();
        for (final String commandId : commandIds) {
            final CommandEntity cmd = this.commandRepo.findOne(commandId);
            if (cmd != null) {
                commandEntities.add(cmd);
            } else {
                throw new GenieNotFoundException("No command with id " + commandId + " exists.");
            }
        }
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
        final ClusterEntity clusterEntity = this.findCluster(id);
        final List<CommandEntity> commandEntities = new ArrayList<>();
        commandEntities.addAll(clusterEntity.getCommands());
        for (final CommandEntity commandEntity : commandEntities) {
            clusterEntity.removeCommand(commandEntity);
        }
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
}
