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
import com.netflix.genie.common.model.Cluster_;
import com.netflix.genie.common.model.Job;
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
import org.springframework.data.domain.PageRequest;
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

        final com.netflix.genie.common.model.Cluster clusterEntity = new com.netflix.genie.common.model.Cluster();
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
    public List<com.netflix.genie.common.dto.Cluster> getClusters(
            final String name,
            final Set<ClusterStatus> statuses,
            final Set<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys
    ) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }

        final PageRequest pageRequest = JPAUtils.getPageRequest(
                page, limit, descending, orderBys, Cluster_.class, Cluster_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<com.netflix.genie.common.model.Cluster> clusters = this.clusterRepo.findAll(
                ClusterSpecs.find(
                        name,
                        statuses,
                        tags,
                        minUpdateTime,
                        maxUpdateTime),
                pageRequest).getContent();
        return clusters.stream().map(com.netflix.genie.common.model.Cluster::getDTO).collect(Collectors.toList());
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
        final Job job = this.jobRepo.findOne(jobId);
        if (job == null) {
            throw new GenieNotFoundException("No job with id " + jobId + " exists. Unable to continue."
            );
        }

        final List<ClusterCriteria> clusterCriterias = job.getClusterCriterias();
        final Set<String> commandCriteria = job.getCommandCriteria();

        for (final ClusterCriteria clusterCriteria : clusterCriterias) {
            @SuppressWarnings("unchecked")
            final List<com.netflix.genie.common.model.Cluster> clusters = this.clusterRepo.findAll(
                    ClusterSpecs.findByClusterAndCommandCriteria(
                            clusterCriteria,
                            commandCriteria
                    )
            );

            if (!clusters.isEmpty()) {
                // Add the successfully criteria to the job object in string form.
                job.setChosenClusterCriteriaString(
                        StringUtils.join(
                                clusterCriteria.getTags(),
                                CRITERIA_DELIMITER
                        )
                );
                return clusters
                        .stream()
                        .map(com.netflix.genie.common.model.Cluster::getDTO)
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
        final com.netflix.genie.common.model.Cluster savedCluster = this.clusterRepo.findOne(id);
        savedCluster.setName(updateCluster.getName());
        savedCluster.setUser(updateCluster.getUser());
        savedCluster.setVersion(updateCluster.getVersion());
        savedCluster.setDescription(updateCluster.getDescription());
        savedCluster.setClusterType(updateCluster.getClusterType());
        savedCluster.setStatus(updateCluster.getStatus());
        savedCluster.setConfigs(updateCluster.getConfigs());
        savedCluster.setTags(updateCluster.getTags());

        this.clusterRepo.save(savedCluster);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllClusters() throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to delete all clusters");
        }
        final List<com.netflix.genie.common.model.Cluster> clusters = this.clusterRepo.findAll();
        for (final com.netflix.genie.common.model.Cluster cluster : clusters) {
            this.deleteCluster(cluster.getId());
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
        final com.netflix.genie.common.model.Cluster cluster = this.findCluster(id);
        final List<com.netflix.genie.common.model.Command> commands = cluster.getCommands();
        if (commands != null) {
            for (final com.netflix.genie.common.model.Command command : commands) {
                final Set<com.netflix.genie.common.model.Cluster> clusters = command.getClusters();
                if (clusters != null) {
                    clusters.remove(cluster);
                }
                this.commandRepo.save(command);
            }
        }
        this.clusterRepo.delete(cluster);
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
        final com.netflix.genie.common.model.Cluster cluster = this.findCluster(id);
        for (final String commandId : commandIds) {
            final com.netflix.genie.common.model.Command cmd = this.commandRepo.findOne(commandId);
            if (cmd != null) {
                cluster.addCommand(cmd);
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
        final com.netflix.genie.common.model.Cluster cluster = this.findCluster(id);
        final List<com.netflix.genie.common.model.Command> commands = cluster.getCommands();
        if (statuses != null) {
            return commands.stream()
                    .filter(command -> statuses.contains(command.getStatus()))
                    .map(com.netflix.genie.common.model.Command::getDTO)
                    .collect(Collectors.toList());
        } else {
            return commands.stream().map(com.netflix.genie.common.model.Command::getDTO).collect(Collectors.toList());
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
        final com.netflix.genie.common.model.Cluster cluster = this.findCluster(id);
        final List<com.netflix.genie.common.model.Command> cmds = new ArrayList<>();
        for (final String commandId : commandIds) {
            final com.netflix.genie.common.model.Command cmd = this.commandRepo.findOne(commandId);
            if (cmd != null) {
                cmds.add(cmd);
            } else {
                throw new GenieNotFoundException("No command with id " + commandId + " exists.");
            }
        }
        cluster.setCommands(cmds);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllCommandsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to remove commands.")
            final String id
    ) throws GenieException {
        final com.netflix.genie.common.model.Cluster cluster = this.findCluster(id);
        final List<com.netflix.genie.common.model.Command> cmdList = new ArrayList<>();
        cmdList.addAll(cluster.getCommands());
        for (final com.netflix.genie.common.model.Command cmd : cmdList) {
            cluster.removeCommand(cmd);
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
        final com.netflix.genie.common.model.Cluster cluster = this.findCluster(id);
        final com.netflix.genie.common.model.Command cmd = this.commandRepo.findOne(cmdId);
        if (cmd != null) {
            cluster.removeCommand(cmd);
        } else {
            throw new GenieNotFoundException("No command with id " + cmdId + " exists.");
        }
    }

    /**
     * Helper method to find a cluster to save code.
     *
     * @param id The id of the cluster to find
     * @return The cluster entity if one exists
     * @throws GenieNotFoundException If the cluster doesn't exist
     */
    private com.netflix.genie.common.model.Cluster findCluster(final String id) throws GenieNotFoundException {
        final com.netflix.genie.common.model.Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster;
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }
}
