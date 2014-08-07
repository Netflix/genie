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

import com.netflix.genie.common.exceptions.GenieException;
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

import java.net.HttpURLConnection;
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
     *
     * @throws GenieException
     */
    @Override
    public Cluster createCluster(final Cluster cluster) throws GenieException {
        if (cluster == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster entered. Unable to validate.");
        }
        cluster.validate();

        LOG.debug("Called to create cluster " + cluster.toString());
        if (StringUtils.isEmpty(cluster.getId())) {
            cluster.setId(UUID.randomUUID().toString());
        }
        if (this.clusterRepo.exists(cluster.getId())) {
            throw new GenieException(
                    HttpURLConnection.HTTP_CONFLICT,
                    "A cluster with id " + cluster.getId() + " already exists");
        }

        return this.clusterRepo.save(cluster);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Cluster getCluster(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered.");
        }
        LOG.debug("Called with id " + id);
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster;
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public List<Cluster> getClusters(
            final String name,
            final List<ClusterStatus> statuses,
            final List<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final int limit,
            final int page) throws GenieException {

        LOG.debug("called");

        final PageRequest pageRequest = new PageRequest(
                page < 0 ? 0 : page,
                limit < 0 ? 1024 : limit,
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
     *
     * @throws GenieException
     */
    @Override
    @Transactional
    public List<Cluster> chooseClusterForJob(
            final String jobId) throws GenieException {
        LOG.debug("Called");
        final Job job = this.jobRepo.findOne(jobId);

        List<ClusterCriteria> clusterCriterias = job.getClusterCriterias();
        Set<String> commandCriteria = job.getCommandCriteria();

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
                job.setChosenClusterCriteriaString(StringUtils.join(clusterCriteria.getTags(), CRITERIA_DELIMITER));
                return clusters;
            }
        }

        //if we've gotten to here no clusters were found so return empty list
        return new ArrayList<Cluster>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Cluster updateCluster(final String id,
                                 final Cluster updateCluster) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update.");
        }
        if (StringUtils.isBlank(updateCluster.getId()) || !id.equals(updateCluster.getId())) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Cluster id either not entered or inconsistent with id passed in.");
        }
        LOG.debug("Called with cluster " + updateCluster.toString());
        final Cluster cluster = this.em.merge(updateCluster);
        cluster.validate();
        return cluster;
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Cluster deleteCluster(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered unable to delete.");
        }
        LOG.debug("Called");
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists to delete.");
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
     *
     * @throws GenieException
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
     *
     * @throws GenieException
     */
    @Override
    public Set<String> addConfigsForCluster(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to add configurations.");
        }
        if (configs == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No configuration files entered.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getConfigs().addAll(configs);
            return cluster.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCluster(
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id sent. Cannot retrieve configurations.");
        }

        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> updateConfigsForCluster(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update configurations.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.setConfigs(configs);
            return cluster.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeAllConfigsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove configs.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getConfigs().clear();
            return cluster.getConfigs();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public List<Command> addCommandsForCluster(
            final String id,
            final List<Command> commands) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to add commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            for (final Command detached : commands) {
                final Command cmd = this.commandRepo.findOne(detached.getId());
                if (cmd != null) {
                    cluster.addCommand(cmd);
                } else {
                    throw new GenieException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No command with id " + detached.getId() + " exists.");
                }
            }
            return cluster.getCommands();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public List<Command> getCommandsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to get commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getCommands();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public List<Command> updateCommandsForCluster(
            final String id,
            final List<Command> commands) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final List<Command> cmds = new ArrayList<Command>();
            for (final Command detached : commands) {
                final Command cmd = this.commandRepo.findOne(detached.getId());
                if (cmd != null) {
                    cmds.add(cmd);
                } else {
                    throw new GenieException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No command with id " + detached.getId() + " exists.");
                }
            }
            cluster.setCommands(cmds);
            return cluster.getCommands();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public List<Command> removeAllCommandsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final List<Command> cmdList = new ArrayList<Command>();
            cmdList.addAll(cluster.getCommands());
            for (final Command cmd : cmdList) {
                cluster.removeCommand(cmd);
            }
            return cluster.getCommands();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public List<Command> removeCommandForCluster(
            final String id,
            final String cmdId) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove command.");
        }
        if (StringUtils.isBlank(cmdId)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove command.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final Command cmd = this.commandRepo.findOne(cmdId);
            if (cmd != null) {
                cluster.removeCommand(cmd);
            } else {
                throw new GenieException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + cmdId + " exists.");
            }
            return cluster.getCommands();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> addTagsForCluster(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to add tags.");
        }
        if (tags == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tags entered.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().addAll(tags);
            return cluster.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForCluster(
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id sent. Cannot retrieve tags.");
        }

        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> updateTagsForCluster(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update tags.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.setTags(tags);
            return cluster.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeAllTagsForCluster(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove tags.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().clear();
            return cluster.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    @Override
    public Set<String> removeTagForCluster(String id, String tag)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove tag.");
        }
        if (StringUtils.isBlank(tag)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tag entered. Unable to remove tag.");
        }
        if (tag.equals(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Cannot delete cluster id from the tags list.");
        }

        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().remove(tag);
            return cluster.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }
}
