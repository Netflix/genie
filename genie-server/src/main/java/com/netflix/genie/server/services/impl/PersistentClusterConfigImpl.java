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
package com.netflix.genie.server.services.impl;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Application_;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Cluster_;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Command_;
import com.netflix.genie.common.model.Types.ClusterStatus;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ClusterConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the PersistentClusterConfig interface.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
public class PersistentClusterConfigImpl implements ClusterConfigService {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistentClusterConfigImpl.class);

    private final PersistenceManager<Cluster> pm;

    /**
     * Default constructor - initialize all required dependencies.
     *
     * @throws CloudServiceException
     */
    public PersistentClusterConfigImpl() throws CloudServiceException {
        // instantiate PersistenceManager
        this.pm = new PersistenceManager<Cluster>();
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Cluster getClusterConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered.");
        }
        LOG.debug("Called with id " + id);
        final EntityManager em = this.pm.createEntityManager();
        try {
            final Cluster cluster = em.find(Cluster.class, id);
            if (cluster != null) {
                return cluster;
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No cluster with id " + id + " exists.");
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public List<Cluster> getClusterConfigs(
            final String name,
            final List<ClusterStatus> statuses,
            final List<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final Integer limit,
            final Integer page) throws CloudServiceException {
        LOG.debug("GENIE: Returning configs for specified params");

        final EntityManager em = this.pm.createEntityManager();
        try {
            final CriteriaBuilder cb = em.getCriteriaBuilder();
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
            final TypedQuery<Cluster> query = em.createQuery(cq);
            final int finalPage = page < 0 ? PersistenceManager.DEFAULT_PAGE_NUMBER : page;
            final int finalLimit = limit < 0 ? PersistenceManager.DEFAULT_PAGE_SIZE : limit;
            query.setMaxResults(finalLimit);
            query.setFirstResult(finalLimit * finalPage);

            //If you want to debug query:
            //LOG.debug(query.unwrap(org.apache.openjpa.persistence.QueryImpl.class).getQueryString());
            return query.getResultList();
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Cluster> getClusterConfigs(
            final String applicationId,
            final String applicationName,
            final String commandId,
            final String commandName,
            final Set<ClusterCriteria> clusterCriterias) {
        LOG.debug("Called");

        final EntityManager em = this.pm.createEntityManager();
        try {
            for (final ClusterCriteria cc : clusterCriterias) {
                final CriteriaBuilder cb = em.getCriteriaBuilder();
                final CriteriaQuery<Cluster> cq = cb.createQuery(Cluster.class);
                final Root<Cluster> c = cq.from(Cluster.class);
                final List<Predicate> predicates = new ArrayList<Predicate>();

                cq.distinct(true);
              
                if (StringUtils.isNotEmpty(commandId) || StringUtils.isNotEmpty(commandName)) {
                    final Join<Cluster, Command> commands = c.join(Cluster_.commands);
                    if (StringUtils.isNotEmpty(commandId)) {
                        predicates.add(cb.equal(commands.get(Command_.id), commandId));
                    } else {
                        predicates.add(cb.equal(commands.get(Command_.name), commandName));
                    }
                    if (StringUtils.isNotEmpty(applicationId) || StringUtils.isNotEmpty(applicationName)) {
                        final Join<Command, Application> apps = commands.join(Command_.applications);
                        if (StringUtils.isNotEmpty(applicationId)) {
                            predicates.add(cb.equal(apps.get(Application_.id), applicationId));
                        } else {
                            predicates.add(cb.equal(apps.get(Application_.name), applicationName));
                        }
                    }
                }
                
                if (cc.getTags() != null) {
                    for (final String tag : cc.getTags()) {
                        predicates.add(cb.isMember(tag, c.get(Cluster_.tags)));
                    }
                }
                
                cq.where(predicates.toArray(new Predicate[0]));
                final TypedQuery<Cluster> query = em.createQuery(cq);
                final List<Cluster> clusters = query.getResultList();

                if (!clusters.isEmpty()) {
                    return clusters;
                }
            }
        } finally {
            em.close();
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
    public Cluster createClusterConfig(final Cluster cluster) throws CloudServiceException {
        Cluster.validate(cluster);
        LOG.debug("Called to create cluster " + cluster.toString());
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            if (StringUtils.isEmpty(cluster.getId())) {
                cluster.setId(UUID.randomUUID().toString());
            }
            if (em.contains(cluster)) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "A cluster with id " + cluster.getId() + " already exists");
            }
            final Set<Command> detachedCommands = cluster.getCommands();
            final Set<Command> attachedCommands = new HashSet<Command>();
            if (detachedCommands != null) {
                for (final Command detached : detachedCommands) {
                    final Command command = em.find(Command.class, detached.getId());
                    if (command != null) {
                        command.getClusters().add(cluster);
                        attachedCommands.add(command);
                    }
                }
            }
            cluster.setCommands(attachedCommands);
            em.persist(cluster);
            trans.commit();
            return cluster;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Cluster updateClusterConfig(final String id,
            final Cluster updateCluster) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster id entered. Unable to update.");
        }
        if (updateCluster == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster information entered. Unable to update.");
        }
        LOG.debug("Called with cluster " + updateCluster.toString());
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Cluster cluster = em.find(Cluster.class, id);
            if (cluster == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No application with id " + id + " exists to update.");
            }
            if (StringUtils.isNotEmpty(updateCluster.getJobManager())) {
                cluster.setJobManager(updateCluster.getJobManager());
            }
            if (StringUtils.isNotEmpty(updateCluster.getName())) {
                cluster.setName(updateCluster.getName());
            }
            if (StringUtils.isNotEmpty(updateCluster.getUser())) {
                cluster.setUser(updateCluster.getUser());
            }
            if (StringUtils.isNotEmpty(updateCluster.getVersion())) {
                cluster.setVersion(updateCluster.getVersion());
            }
            if (updateCluster.getStatus() != null
                    && updateCluster.getStatus() != cluster.getStatus()) {
                cluster.setStatus(updateCluster.getStatus());
            }
            Cluster.validate(cluster);
            trans.commit();
            return cluster;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public Cluster deleteClusterConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered unable to delete.");
        }
        LOG.debug("Called");
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            final Cluster cluster = em.find(Cluster.class, id);
            if (cluster == null) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No cluster with id " + id + " exists to delete.");
            }
            final Set<Command> commands = cluster.getCommands();
            if (commands != null) {
                for (final Command command : commands) {
                    final Set<Cluster> clusters = command.getClusters();
                    if (clusters != null) {
                        clusters.remove(cluster);
                    }
                }
            }
            em.remove(cluster);
            trans.commit();
            return cluster;
        } finally {
            if (trans.isActive()) {
                trans.rollback();
            }
            em.close();
        }
    }
}
