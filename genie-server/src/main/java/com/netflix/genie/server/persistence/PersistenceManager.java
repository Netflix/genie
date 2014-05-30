/*
 *
 *  Copyright 2013 Netflix, Inc.
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
package com.netflix.genie.server.persistence;

import com.netflix.genie.common.exceptions.CloudServiceException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic PersistenceManager utility class that will be shared across various
 * services.
 *
 * @author skrishnan
 * @author bmundlapudi
 *
 * @param <T> the class that must be persisted in the database
 */
public class PersistenceManager<T> {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistenceManager.class);

    // a global instance-wide lock to ensure that no two threads
    // are writing at the same time
    // we may need a distributed (global) lock in the future
    private static final ReentrantReadWriteLock DB_LOCK = new ReentrantReadWriteLock();

    // a static instance to the factory, which initializes the connection pool
    // we only need one instance of this factory per JVM
    // it is volatile to ensure proper double-checked locking
    private static volatile EntityManagerFactory entityManagerFactory;

    /**
     * The alias to be used to represent the entity for the query.
     */
    public static final String ENTITY_ALIAS = "T";

    /**
     * The maximum number of entries to be returned.
     */
    public static final int MAX_PAGE_SIZE = 1024;

    /**
     * Default constructor.
     */
    public PersistenceManager() {
    }

    /**
     * Shuts down the persistence manager cleanly.
     */
    public static synchronized void shutdown() {
        if (entityManagerFactory.isOpen()) {
            entityManagerFactory.close();
        }
    }

    /**
     * Get the db lock for this instance.
     *
     * @return reference to the database lock shared by this instance
     */
    public static ReentrantReadWriteLock getDbLock() {
        LOG.debug("called");
        return DB_LOCK;
    }

    /**
     * Persist an entity in the database.
     *
     * @param entity element to store in database
     */
    public void createEntity(T entity) {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            em.getTransaction().begin();
            em.persist(entity);
            em.getTransaction().commit();
        } finally {
            em.close();
        }
    }

    /**
     * Query the database using the specified query builder.
     *
     * @param builder object encapsulating the query
     * @return array of objects satisfying given criteria
     */
    @SuppressWarnings("unchecked")
    public T[] query(QueryBuilder builder) {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            String strQuery;
            String table = builder.getTable();
            String criteria = builder.getClause();
            strQuery = ((criteria == null) || criteria.isEmpty()) ? String
                    .format("select %s from %s %s", ENTITY_ALIAS, table,
                            ENTITY_ALIAS) : String.format(
                            "select %s from %s %s where %s", ENTITY_ALIAS, table,
                            ENTITY_ALIAS, criteria);

            // order by update time, if need be
            boolean orderByUpdateTime = builder.isOrderByUpdateTime();
            if (orderByUpdateTime) {
                boolean desc = builder.isDesc();
                if (desc) {
                    strQuery += String.format(" order by %s.updated desc",
                            ENTITY_ALIAS);
                } else {
                    strQuery += String.format(" order by %s.updated asc",
                            ENTITY_ALIAS);
                }
            }

            Query q = em.createQuery(strQuery);
            LOG.debug("Query string: " + strQuery);

            // set max results to default, or requested limit (capped by MAX)
            // for paginated results, limit is per page
            Integer limit = builder.getLimit();
            if ((limit == null) || (limit > MAX_PAGE_SIZE)) {
                limit = MAX_PAGE_SIZE;
            }
            boolean paginate = builder.isPaginate();
            if (paginate) {
                q.setMaxResults(limit);
            }

            // enable pagination, if requested
            Integer page = builder.getPage();
            if (paginate && (page != null)) {
                int startPos = getStartPosition(page, limit);
                q.setFirstResult(startPos);
            }

            List<T> records = (List<T>) q.getResultList();
            T[] results = (T[]) new Object[records.size()];
            results = (T[]) records.toArray(results);
            return results;
        } finally {
            em.close();
        }
    }

    /**
     * Update a set of rows based on the given criteria.
     *
     * @param builder object encapsulating querying, having both a "set" and a
     * "clause"
     * @return number of rows updated
     * @throws CloudServiceException
     */
    public int update(QueryBuilder builder) throws CloudServiceException {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            String strQuery;
            String table = builder.getTable();
            String set = builder.getSet();
            if ((set == null) || (set.isEmpty())) {
                String msg = "Set can't be empty/null in an update statement";
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST, msg);
            }
            String criteria = builder.getClause();
            strQuery = ((criteria == null) || criteria.isEmpty()) ? String
                    .format("update %s %s set %s", table, ENTITY_ALIAS, set)
                    : String.format("update %s %s set %s where %s", table,
                            ENTITY_ALIAS, set, criteria);
            Query q = em.createQuery(strQuery);
            em.getTransaction().begin();
            int numRows = q.executeUpdate();
            em.getTransaction().commit();
            return numRows;
        } finally {
            em.close();
        }
    }

    /**
     * Update an entity in the database.
     *
     * @param entity the entity to update
     * @return updated entity
     */
    public T updateEntity(T entity) {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            em.getTransaction().begin();
            T result = em.merge(entity);
            em.getTransaction().commit();
            return result;
        } finally {
            em.close();
        }
    }

    /**
     * Delete an entity from the database.
     *
     * @param id entity to delete from database
     * @param type type of entity to delete
     * @return deleted entity
     */
    public T deleteEntity(String id, Class<T> type) {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            em.getTransaction().begin();
            T entity = getEntity(id, type, em);
            if (entity != null) {
                em.remove(entity);
            }
            em.getTransaction().commit();

            return entity;
        } finally {
            em.close();
        }
    }

    /**
     * Get an entity from the database.
     *
     * @param id the id to look up in the database
     * @param type class name for the entity to look up
     * @param em reference to the entity manager
     * @return the entity returned by the database
     */
    private T getEntity(String id, Class<T> type, EntityManager em) {
        LOG.debug("called");
        try {
            return type.cast(em.find(type, id));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get an entity from the database.
     *
     * @param id the id to look up in the database
     * @param type class name for the entity to look up
     * @return the entity returned by the database
     */
    public T getEntity(String id, Class<T> type) {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            return type.cast(em.find(type, id));
        } finally {
            em.close();
        }
    }

    /**
     * Thread-safe creation of entity manager since the createEntityManager() is
     * proving to be unsafe.
     *
     * @return entity manager object
     */
    public EntityManager createEntityManager() {
        synchronized (PersistenceManager.class) {
            if ((entityManagerFactory == null) || (!entityManagerFactory.isOpen())) {
                String persistenceUnitName = "genie";
                LOG.info("Initializing PersistenceManager: "
                        + persistenceUnitName);
                entityManagerFactory = Persistence
                        .createEntityManagerFactory(persistenceUnitName);
            }
            return entityManagerFactory.createEntityManager();
        }
    }

    /**
     * Get the start position to return rows from.
     *
     * @param pageNumber the page number to return from
     * @param limit number of entries per page
     * @return the start position
     */
    private int getStartPosition(int pageNumber, int limit) {
        int startPos = 0;
        if (pageNumber >= 0) {
            startPos = pageNumber * limit;
        }
        return startPos;
    }
}
