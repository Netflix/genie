/*
 *
 *  Copyright 2014 Netflix, Inc.
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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;

import com.netflix.genie.common.model.Cluster_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.ClusterStatus;

import com.netflix.genie.server.repository.jpa.ClusterRepository;
import com.netflix.genie.server.repository.jpa.ClusterSpecs;
import com.netflix.genie.server.repository.jpa.CommandRepository;
import com.netflix.genie.server.repository.jpa.JobRepository;
import com.netflix.genie.server.services.ClusterConfigService;
import org.springframework.data.domain.Sort.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.lang3.StringUtils;
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
@Named
@Transactional(rollbackFor = GenieException.class)
public class ClusterConfigServiceJPAImpl implements ClusterConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigServiceJPAImpl.class);

    @PersistenceContext
    private EntityManager em;

    private final ClusterRepository clusterRepo;
    private final CommandRepository commandRepo;
    private final JobRepository jobRepo;
    private static final char CRITERIA_DELIMITER = ',';

    /**
     * Default constructor - initialize all required dependencies.
     *
     * @param clusterRepo The cluster repository to use.
     * @param commandRepo the command repository to use.
     * @param jobRepo The job repository to use.
     */
    @Inject
    public ClusterConfigServiceJPAImpl(
            final ClusterRepository clusterRepo,
            final CommandRepository commandRepo,
            final JobRepository jobRepo) {
        this.clusterRepo = clusterRepo;
        this.commandRepo = commandRepo;
        this.jobRepo = jobRepo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster createCluster(final Cluster cluster) throws GenieException {
        if (cluster == null) {
            throw new GeniePreconditionException("No cluster entered. Unable to validate.");
        }
        cluster.validate();

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
    public Cluster getCluster(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GeniePreconditionException("No id entered.");
        }
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
            final int limit) {

        LOG.debug("called");

        final PageRequest pageRequest = new PageRequest(
                page < 0 ? 0 : page,
                limit < 1 ? 1024 : limit,
                Direction.DESC,
                Cluster_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Cluster> clusters = this.clusterRepo.findAll(
                ClusterSpecs.findByNameAndStatusesAndTagsAndUpdateTime(
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
            final String jobId) throws GenieException {
        LOG.debug("Called");
        if (StringUtils.isBlank(jobId)) {
            throw new GeniePreconditionException("No job id entered. Unable to continue"
            );
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
            final List<Cluster> clusters = this.clusterRepo.findAll(
                    ClusterSpecs.findByClusterAndCommandCriteria(
                            clusterCriteria,
                            commandCriteria
                    )
            );

            if (!clusters.isEmpty()) {
                // Add the succesfully criteria to the job object in string form.
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
            final String id,
            final Cluster updateCluster) throws GenieException {
        LOG.debug("Called with id " + id + " and cluster " + updateCluster);
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to update.");
        }
        if (updateCluster == null) {
            throw new GeniePreconditionException("No cluster information entered. Unable to update.");
        }
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
        final Cluster cluster = this.em.merge(updateCluster);
        cluster.validate();
        return cluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster deleteCluster(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GeniePreconditionException("No id entered unable to delete.");
        }
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
    public Set<String> addConfigsForCluster(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to add configurations.");
        }
        if (configs == null) {
            throw new GeniePreconditionException("No configuration files entered.");
        }
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
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id sent. Cannot retrieve configurations.");
        }

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
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to update configurations.");
        }
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
            final String id,
            final List<Command> commands) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to add commands.");
        }
        if (commands == null) {
            throw new GeniePreconditionException("No commands entered. Unable to add commands.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to get commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getCommands();
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
            final String id,
            final List<Command> commands) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to update commands.");
        }
        if (commands == null) {
            throw new GeniePreconditionException("No commands entered. Unable to add commands.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to remove commands.");
        }
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
            final String id,
            final String cmdId) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to remove command.");
        }
        if (StringUtils.isBlank(cmdId)) {
            throw new GeniePreconditionException("No command id entered. Unable to remove command.");
        }
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
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to add tags.");
        }
        if (tags == null) {
            throw new GeniePreconditionException("No tags entered.");
        }
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
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id sent. Cannot retrieve tags.");
        }

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
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to update tags.");
        }
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
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to remove tags.");
        }
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
    public Set<String> removeTagForCluster(final String id, final String tag)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No cluster id entered. Unable to remove tag.");
        }
        if (id.equals(tag)) {
            throw new GeniePreconditionException("Cannot delete cluster id from the tags list.");
        }

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
