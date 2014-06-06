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
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Types.ClusterStatus;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ClusterConfigService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;
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
    //TODO: Get this out of the query/clause builder so class can be deleted
    public List<Cluster> getClusterConfigs(
            final String name,
            final List<String> statuses, //TODO: Why can't this be the ENUM already?
            final List<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final Integer limit,
            final Integer page) throws CloudServiceException {
        LOG.debug("Called");

        LOG.debug("GENIE: Returning configs for specified params");

        // construct query
        ClauseBuilder criteria = new ClauseBuilder(ClauseBuilder.AND);
        if (StringUtils.isNotEmpty(name)) {
            criteria.append("name like '" + name + "'");
        }
        if (minUpdateTime != null) {
            criteria.append("updated >= " + minUpdateTime);
        }
        if (maxUpdateTime != null) {
            criteria.append("updated <= " + maxUpdateTime);
        }

        if (tags != null) {
            for (final String tag : tags) {
                criteria.append("\"" + tag + "\" member of T.tags", false);
            }
        }

        if (statuses != null && !statuses.isEmpty()) {
            int count = 0;
            final ClauseBuilder statusCriteria = new ClauseBuilder(ClauseBuilder.OR);
            for (final String status : statuses) {
                if (StringUtils.isEmpty(status)) {
                    continue;
                }
                statusCriteria.append("status = '" + ClusterStatus.parse(status) + "'");
                count++;
            }
            if (count > 0) {
                criteria.append("(" + statusCriteria.toString() + ")", false);
            }
        }

        // Get all the results as an array
        final String criteriaString = criteria.toString();
        LOG.debug("Criteria: " + criteriaString);
        QueryBuilder builder = new QueryBuilder()
                .table("Cluster").clause(criteriaString)
                .limit(limit).page(page);
        return Arrays.asList(this.pm.query(builder));
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
        //TODO: Do we want to log out anything else here?
        LOG.debug("Called");

        final EntityManager em = this.pm.createEntityManager();
        try {
            for (final ClusterCriteria cc : clusterCriterias) {
                final StringBuilder builder = new StringBuilder();
                builder.append("SELECT distinct cstr FROM Cluster cstr, IN(cstr.commands) cmds ");

                if (StringUtils.isNotEmpty(applicationId)) {
                    builder.append(", IN(cmds.applications) apps ");
                } else if (StringUtils.isNotEmpty(applicationName)) {
                    builder.append(", IN(cmds.applications) apps ");
                }

                builder.append(" WHERE ");

                if (cc.getTags() != null) {
                    for (int i = 0; i < cc.getTags().size(); i++) {
                        if (i != 0) {
                            builder.append(" AND ");
                        }

                        builder.append(":tag").append(i).append(" member of cstr.tags ");
                    }
                }

                if (StringUtils.isNotEmpty(commandId)) {
                    builder.append(" AND cmds.id = \"").append(commandId).append("\" ");
                } else if (StringUtils.isNotEmpty(commandName)) {
                    builder.append(" AND cmds.name = \"").append(commandName).append("\" ");
                }

                if ((applicationId != null) && (!applicationId.isEmpty())) {
                    builder.append(" AND apps.id = \"").append(applicationId).append("\" ");
                } else if ((applicationName != null) && (!applicationName.isEmpty())) {
                    builder.append(" AND apps.name = \"").append(applicationName).append("\" ");
                }

                final String queryString = builder.toString();
                LOG.debug("Query is " + queryString);

                final TypedQuery<Cluster> query = em.createQuery(queryString, Cluster.class);

                if (cc.getTags() != null) {
                    int tagNum = 0;
                    for (final String tag : cc.getTags()) {
                        query.setParameter("tag" + tagNum, tag);
                        tagNum++;
                    }
                }

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
        if (cluster == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster entered. Unable to create");
        }
        LOG.info("Called");
        if (StringUtils.isEmpty(cluster.getId())) {
            cluster.setId(UUID.randomUUID().toString());
        }
        final EntityManager em = this.pm.createEntityManager();
        final EntityTransaction trans = em.getTransaction();
        try {
            trans.begin();
            if (em.contains(cluster)) {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "A cluster with id " + cluster.getId() + " already exists");
            }
            if (StringUtils.isEmpty(cluster.getId())) {
                cluster.setId(UUID.randomUUID().toString());
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
