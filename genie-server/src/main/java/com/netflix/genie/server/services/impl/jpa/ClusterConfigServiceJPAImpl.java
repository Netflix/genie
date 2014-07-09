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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Cluster_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Command_;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.ClusterStatus;
import com.netflix.genie.common.model.Types.CommandStatus;
import com.netflix.genie.server.repository.jpa.ClusterRepository;
import com.netflix.genie.server.repository.jpa.CommandRepository;
import com.netflix.genie.server.services.ClusterConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of the PersistentClusterConfig interface.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
@Named
@Transactional(rollbackFor = CloudServiceException.class)
public class ClusterConfigServiceJPAImpl implements ClusterConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigServiceJPAImpl.class);

    @PersistenceContext
    private EntityManager em;

    private final ClusterRepository clusterRepo;
    private final CommandRepository commandRepo;
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
            final CommandRepository commandRepo) {
        this.clusterRepo = clusterRepo;
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Cluster createCluster(final Cluster cluster) throws CloudServiceException {
        Cluster.validate(cluster);
        LOG.debug("Called to create cluster " + cluster.toString());
        if (StringUtils.isEmpty(cluster.getId())) {
            cluster.setId(UUID.randomUUID().toString());
        }
        if (this.clusterRepo.exists(cluster.getId())) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "A cluster with id " + cluster.getId() + " already exists");
        }

        return this.clusterRepo.save(cluster);
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Cluster getCluster(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered.");
        }
        LOG.debug("Called with id " + id);
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster;
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }
    
    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
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
            final int page) throws CloudServiceException {
        LOG.debug("GENIE: Returning configs for specified params");
        final CriteriaBuilder cb = this.em.getCriteriaBuilder();
        final CriteriaQuery<Cluster> cq = cb.createQuery(Cluster.class);
        final Root<Cluster> c = cq.from(Cluster.class);
        final List<Predicate> predicates = new ArrayList<Predicate>();
        if (StringUtils.isNotEmpty(name)) {
            predicates.add(cb.like(c.get(Cluster_.name), name));
        }
        if (minUpdateTime != null) {
            predicates.add(cb.greaterThanOrEqualTo(c.get(Cluster_.updated), new Date(minUpdateTime)));
        }
        if (maxUpdateTime != null) {
            predicates.add(cb.lessThan(c.get(Cluster_.updated), new Date(maxUpdateTime)));
        }
        if (tags != null) {
            for (final String tag : tags) {
                predicates.add(cb.isMember(tag, c.get(Cluster_.tags)));
            }
        }

        if (statuses != null && !statuses.isEmpty()) {
            //Could optimize this as we know size could use native array
            final List<Predicate> orPredicates = new ArrayList<Predicate>();
            for (final ClusterStatus status : statuses) {
                orPredicates.add(cb.equal(c.get(Cluster_.status), status));
            }
            predicates.add(cb.or(orPredicates.toArray(new Predicate[0])));
        }

        cq.where(predicates.toArray(new Predicate[0]));
        final TypedQuery<Cluster> query = this.em.createQuery(cq);
        final int finalPage = page < 0 ? 0 : page;
        final int finalLimit = limit < 0 ? 1024 : limit;
        query.setMaxResults(finalLimit);
        query.setFirstResult(finalLimit * finalPage);

        //If you want to debug query:
        //LOG.debug(query.unwrap(org.apache.openjpa.persistence.QueryImpl.class).getQueryString());
        return query.getResultList();
    }

    /**
     * {@inheritDoc}
     * @throws CloudServiceException 
     */
    @Override
    @Transactional(readOnly = true)
    public List<Cluster> getClusters(
            final Job job) throws CloudServiceException {
        LOG.debug("Called");
        if (job == null) {
            final String msg = "No job entered. Unable to continue";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        
        List<ClusterCriteria> clusterCriterias = job.getClusterCriteria();
        Set<String> commandCriteria = job.getCommandCriteria();
        
        for (final ClusterCriteria cc : clusterCriterias) {
            final CriteriaBuilder cb = this.em.getCriteriaBuilder();
            final CriteriaQuery<Cluster> cq = cb.createQuery(Cluster.class);
            final Root<Cluster> c = cq.from(Cluster.class);
            final List<Predicate> predicates = new ArrayList<Predicate>();
            final Join<Cluster, Command> commands = c.join(Cluster_.commands);
            
            cq.distinct(true);

            predicates.add(cb.equal(commands.get(Command_.status), CommandStatus.ACTIVE));
            predicates.add(cb.equal(c.get(Cluster_.status), ClusterStatus.UP));
 
            if (commandCriteria != null) {
                for (final String tag: commandCriteria) {
                    predicates.add(cb.isMember(tag, commands.get(Command_.tags)));
                }
            }
            
            if (cc.getTags() != null) {
                for (final String tag : cc.getTags()) {
                    predicates.add(cb.isMember(tag, c.get(Cluster_.tags)));
                }
            }

            cq.where(predicates.toArray(new Predicate[0]));
            final TypedQuery<Cluster> query = this.em.createQuery(cq);
            final List<Cluster> clusters = query.getResultList();

            if (!clusters.isEmpty()) {
                // Add the succesfully criteria to the job object in string form.
                job.setChosenClusterCriteriaString(StringUtils.join(cc.getTags(), CRITERIA_DELIMITER));
                return clusters;
            }
        }

        //if we've gotten to here no clusters were found so return empty list
        return new ArrayList<Cluster>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Cluster updateCluster(final String id,
            final Cluster updateCluster) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update.");
        }
        if (StringUtils.isBlank(updateCluster.getId()) || !id.equals(updateCluster.getId())) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Cluster id either not entered or inconsistent with id passed in.");
        }
        LOG.debug("Called with cluster " + updateCluster.toString());
        final Cluster cluster = this.em.merge(updateCluster);
        Cluster.validate(cluster);
        return cluster;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Cluster deleteCluster(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered unable to delete.");
        }
        LOG.debug("Called");
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster == null) {
            throw new CloudServiceException(
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
     * @throws CloudServiceException
     */
    @Override
    public List<Cluster> deleteAllClusters() throws CloudServiceException {
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
     * @throws CloudServiceException
     */
    @Override
    public Set<String> addConfigsForCluster(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to add configurations.");
        }
        if (configs == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No configuration files entered.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getConfigs().addAll(configs);
            return cluster.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getConfigsForCluster(
            final String id)
            throws CloudServiceException {

        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id sent. Cannot retrieve configurations.");
        }

        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> updateConfigsForCluster(
            final String id,
            final Set<String> configs) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update configurations.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.setConfigs(configs);
            return cluster.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> removeAllConfigsForCluster(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove configs.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getConfigs().clear();
            return cluster.getConfigs();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Command> addCommandsForCluster(
            final String id,
            final List<Command> commands) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
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
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No command with id " + detached.getId() + " exists.");
                }
            }
            return cluster.getCommands();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public List<Command> getCommandsForCluster(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to get commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getCommands();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Command> updateCommandsForCluster(
            final String id,
            final List<Command> commands) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
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
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No command with id " + detached.getId() + " exists.");
                }
            }
            cluster.setCommands(cmds);
            return cluster.getCommands();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Command> removeAllCommandsForCluster(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove commands.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            ArrayList<Command> cmdList = new ArrayList<Command>();
            cmdList.addAll(cluster.getCommands());
            for (final Command cmd : cmdList) {
                cluster.removeCommand(cmd);
            }
            return cluster.getCommands();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Command> removeCommandForCluster(
            final String id,
            final String cmdId) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove command.");
        }
        if (StringUtils.isBlank(cmdId)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered. Unable to remove command.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            final Command cmd = this.commandRepo.findOne(cmdId);
            if (cmd != null) {
                cluster.removeCommand(cmd);
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No command with id " + cmdId + " exists.");
            }
            return cluster.getCommands();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }
    
    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> addTagsForCluster(
            final String id,
            final Set<String> tags) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to add tags.");
        }
        if (tags == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tags entered.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().addAll(tags);
            return cluster.getTags();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForCluster(
            final String id)
            throws CloudServiceException {

        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id sent. Cannot retrieve tags.");
        }

        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            return cluster.getTags();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> updateTagsForCluster(
            final String id,
            final Set<String> tags) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update tags.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.setTags(tags);
            return cluster.getTags();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Set<String> removeAllTagsForCluster(
            final String id) throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove tags.");
        }
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().clear();
            return cluster.getTags();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }

    @Override
    public Set<String> removeTagForCluster(String id, String tag)
            throws CloudServiceException {
        if (StringUtils.isBlank(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to remove tag.");
        }
        if (StringUtils.isBlank(tag)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tag entered. Unable to remove tag.");
        }
        if (tag.equals(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Cannot delete cluster id from the tags list.");
        }
        
        final Cluster cluster = this.clusterRepo.findOne(id);
        if (cluster != null) {
            cluster.getTags().remove(tag);
            return cluster.getTags();
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No cluster with id " + id + " exists.");
        }
    }
}
