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
package com.netflix.genie.server.persistence;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
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
     * The maximum number of entries to be returned.
     */
    public static final int DEFAULT_PAGE_SIZE = 1024;
    
    /**
     * The default page number for range queries
     */
    public static final int DEFAULT_PAGE_NUMBER = 0;

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
    public void createEntity(final T entity) {
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
     * Get an entity from the database.
     *
     * @param id the id to look up in the database
     * @param type class name for the entity to look up
     * @return the entity returned by the database
     */
    public T getEntity(final String id, final Class<T> type) {
        LOG.debug("called");
        EntityManager em = createEntityManager();
        try {
            return (T) em.find(type, id);
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
    public T updateEntity(final T entity) {
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
}
