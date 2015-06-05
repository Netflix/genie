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
package com.netflix.genie.server.services.impl.jpa;

import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;

import com.netflix.genie.common.model.Cluster_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.CommandStatus;

import com.netflix.genie.server.repository.jpa.*;
import com.netflix.genie.server.services.ClusterConfigService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;

import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of the PersistentClusterConfig interface.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
@Transactional(
        rollbackFor = {
                GenieException.class,
                ConstraintViolationException.class
        }
)
public class ClusterConfigServiceJPAImpl implements ClusterConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigServiceJPAImpl.class);
    private static final char CRITERIA_DELIMITER = ',';
    private final ClusterRepository clusterRepo;
    private final CommandRepository commandRepo;
    private final JobRepository jobRepo;
    @PersistenceContext
    private EntityManager em;

    /**
     * Default constructor - initialize all required dependencies.
     *
     * @param clusterRepo The cluster repository to use.
     * @param commandRepo The command repository to use.
     * @param jobRepo     The job repository to use.
     */
    public ClusterConfigServiceJPAImpl(
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
    public Cluster createCluster(
            @NotNull(message = "No cluster entered. Unable to create.")
            @Valid
            final Cluster cluster
    ) throws GenieException {
        LOG.debug("Called to create cluster " + cluster.toString());
        if (StringUtils.isEmpty(cluster.getId())) {
            cluster.setId(UUID.randomUUID().toString());
        }
        if (this.clusterRepo.exists(cluster.getId())) {
            throw new GenieConflictException("A cluster with id " + cluster.getId() + " already exists");
        }

        return this.clusterRepo.save(cluster);
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
        LOG.debug("Called with id " + id);
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster;
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Cluster> getClusters(
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
        LOG.debug("called");

        final PageRequest pageRequest = JPAUtils.getPageRequest(
                page, limit, descending, orderBys, Cluster_.class, Cluster_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Cluster> clusters = this.clusterRepo.findAll(
                ClusterSpecs.find(
                        name,
                        statuses,
                        tags,
                        minUpdateTime,
                        maxUpdateTime),
                pageRequest).getContent();
        return clusters;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Cluster> chooseClusterForJob(
            @NotBlank(message = "No job id entered. Unable to continue.")
            final String jobId
    ) throws GenieException {
        LOG.debug("Called");
        final Job job = this.jobRepo.findOne(jobId);
        if (job == null) {
            throw new GenieNotFoundException("No job with id " + jobId + " exists. Unable to continue."
            );
        }

        final List<ClusterCriteria> clusterCriterias = job.getClusterCriterias();
        final Set<String> commandCriteria = job.getCommandCriteria();

        for (final ClusterCriteria clusterCriteria : clusterCriterias) {
            @SuppressWarnings("unchecked")
            final List<Cluster> clusters = this.clusterRepo.findAll(
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
                return clusters;
            }
        }

        //if we've gotten to here no clusters were found so return empty list
        return new ArrayList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster updateCluster(
            @NotBlank(message = "No cluster id entered. Unable to update.")
            final String id,
            @NotNull(message = "No cluster information entered. Unable to update.")
            final Cluster updateCluster
    ) throws GenieException {
        LOG.debug("Called with id " + id + " and cluster " + updateCluster);
        if (!this.clusterRepo.exists(id)) {
            throw new GenieNotFoundException("No cluster exists with the given id. Unable to update.");
        }
        if (StringUtils.isNotBlank(updateCluster.getId())
                && !id.equals(updateCluster.getId())) {
            throw new GenieBadRequestException("Cluster id inconsistent with id passed in.");
        }

        //Set the id if it's not set so we can merge
        if (StringUtils.isBlank(updateCluster.getId())) {
            updateCluster.setId(id);
        }
        return this.em.merge(updateCluster);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Cluster> deleteAllClusters() throws GenieException {
        LOG.debug("Called to delete all clusters");
        final List<Cluster> clusters = this.clusterRepo.findAll();
        for (final Cluster cluster : clusters) {
            this.deleteCluster(cluster.getId());
        }
        return clusters;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster deleteCluster(
            @NotBlank(message = "No id entered unable to delete.")
            final String id
    ) throws GenieException {
        LOG.debug("Called");
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster == null) {
            throw new GenieNotFoundException("No cluster with id " + id + " exists to delete.");
        }
        final List<Command> commands = cluster.getCommands();
        if (commands != null) {
            for (final Command command : commands) {
                final Set<Cluster> clusters = command.getClusters();
                if (clusters != null) {
                    clusters.remove(cluster);
                }
            }
        }
        this.clusterRepo.delete(cluster);
        return cluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> addConfigsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to add configurations.")
            final String id,
            @NotEmpty(message = "No configuration files entered. Unable to add.")
            final Set<String> configs
    ) throws GenieException {
        LOG.debug("called");
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getConfigs().addAll(configs);
            return cluster.getConfigs();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
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
        LOG.debug("called");
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getConfigs();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateConfigsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to update configurations.")
            final String id,
            @NotEmpty(message = "No configs entered. Unable to update.")
            final Set<String> configs
    ) throws GenieException {
        LOG.debug("called with id " + id + " and configs " + configs);
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.setConfigs(configs);
            return cluster.getConfigs();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Command> addCommandsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to add commands.")
            final String id,
            @NotEmpty(message = "No commands entered. Unable to add commands.")
            final List<Command> commands
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            for (final Command detached : commands) {
                final Command cmd = this.commandRepo.findOne(detached.getId());
                if (cmd != null) {
                    cluster.addCommand(cmd);
                } else {
                    throw new GenieNotFoundException("No command with id " + detached.getId() + " exists.");
                }
            }
            return cluster.getCommands();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
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
        final Cluster cluster = this.clusterRepo.findOne(id);
        List<Command> filteredCommandList = new ArrayList<Command>();

        if (cluster != null) {
            List<Command> commands = cluster.getCommands();
            if (statuses != null) {
                for (Command command: commands) {
                    if (statuses.contains(command.getStatus())) {
                        filteredCommandList.add(command);
                    }
                }
                return filteredCommandList;
            } else {
                return commands;
            }

        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    //TODO: Shares a lot of code with the add, should be able to refactor
    public List<Command> updateCommandsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to update commands.")
            final String id,
            @NotEmpty(message = "No commands entered. Unable to add commands.")
            final List<Command> commands
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final List<Command> cmds = new ArrayList<>();
            for (final Command detached : commands) {
                final Command cmd = this.commandRepo.findOne(detached.getId());
                if (cmd != null) {
                    cmds.add(cmd);
                } else {
                    throw new GenieNotFoundException("No command with id " + detached.getId() + " exists.");
                }
            }
            cluster.setCommands(cmds);
            return cluster.getCommands();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Command> removeAllCommandsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to remove commands.")
            final String id
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final List<Command> cmdList = new ArrayList<>();
            cmdList.addAll(cluster.getCommands());
            for (final Command cmd : cmdList) {
                cluster.removeCommand(cmd);
            }
            return cluster.getCommands();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Command> removeCommandForCluster(
            @NotBlank(message = "No cluster id entered. Unable to remove command.")
            final String id,
            @NotBlank(message = "No command id entered. Unable to remove command.")
            final String cmdId
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final Command cmd = this.commandRepo.findOne(cmdId);
            if (cmd != null) {
                cluster.removeCommand(cmd);
            } else {
                throw new GenieNotFoundException("No command with id " + cmdId + " exists.");
            }
            return cluster.getCommands();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> addTagsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to add to tags.")
            final Set<String> tags
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().addAll(tags);
            return cluster.getTags();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
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
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getTags();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> updateTagsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to update tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to update.")
            final Set<String> tags
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.setTags(tags);
            return cluster.getTags();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeAllTagsForCluster(
            @NotBlank(message = "No cluster id entered. Unable to remove tags.")
            final String id
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().clear();
            return cluster.getTags();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeTagForCluster(
            @NotBlank(message = "No cluster id entered. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove.")
            final String tag
    ) throws GenieException {
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            if (StringUtils.isNotBlank(tag)) {
                cluster.getTags().remove(tag);
            }
            return cluster.getTags();
        } else {
            throw new GenieNotFoundException("No cluster with id " + id + " exists.");
        }
    }
}
