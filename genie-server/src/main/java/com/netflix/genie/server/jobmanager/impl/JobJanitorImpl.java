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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.common.model.Types.SubprocessStatus;
import com.netflix.genie.server.jobmanager.JobJanitor;
import java.util.Date;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.transaction.annotation.Transactional;

/**
 * Janitor thread that marks jobs as zombies if status hasn't been updated for
 * the configured timeout.
 *
 * @author skrishnan
 * @author tgianos
 */
@Named
@Scope("prototype")
public class JobJanitorImpl implements JobJanitor {

    private static final Logger LOG = LoggerFactory.getLogger(JobJanitorImpl.class);

    private final AbstractConfiguration conf;
    private boolean stop;
    
    @PersistenceContext
    private EntityManager em;

    /**
     * Default constructor - initializes members correctly in order.
     */
    public JobJanitorImpl() {
        this.conf = ConfigurationManager.getConfigInstance();
        this.stop = false;
    }

    /**
     * {@inheritDoc}
     * @throws Exception 
     */
    @Override
    @Transactional
    public int markZombies() throws Exception {
        // the equivalent query is as follows:
        // update Job set status='FAILED', finishTime=$max, exitCode=$zombie_code,
        // statusMsg='Job has been marked as a zombie'
        // where updateTime < $min and (status='RUNNING' or status='INIT')"
        long currentTime = System.currentTimeMillis();
        long zombieTime = this.conf.getLong(
                "netflix.genie.server.janitor.zombie.delta.ms", 1800000);

        final StringBuilder builder = new StringBuilder();
        //TODO: Replace with criteria query
        builder.append("UPDATE Job j ");
        builder.append("SET j.status = :failed, j.finishTime = :currentTime, j.exitCode = :exitCode, j.statusMsg = :statusMessage ");
        builder.append("WHERE j.updated < :updated AND (j.status = :running OR j.status = :init)");
        try {
            final Query query = this.em.createQuery(builder.toString())
                    .setParameter("failed", JobStatus.FAILED)
                    .setParameter("currentTime", currentTime)
                    .setParameter("exitCode", SubprocessStatus.ZOMBIE_JOB.code())
                    .setParameter("statusMessage", SubprocessStatus.message(SubprocessStatus.ZOMBIE_JOB.code()))
                    .setParameter("updated", new Date((currentTime - zombieTime)))
                    .setParameter("running", JobStatus.RUNNING)
                    .setParameter("init", JobStatus.INIT);
            final EntityTransaction trans = em.getTransaction();
            trans.begin();
            final int rowsUpdated = query.executeUpdate();
            trans.commit();
            return rowsUpdated;
        } finally {
            em.close();
        }
    }

    /**
     * The main run method for this thread - it runs for ever until explicitly
     * shutdown.
     */
    @Override
    public void run() {
        while (true) {
            LOG.info("Job janitor daemon waking up");
            if (this.stop) {
                LOG.info("Job janitor stopping as per request");
                return;
            }

            try {
                int numRowsUpdated = markZombies();
                LOG.info("Total jobs marked as zombies: " + numRowsUpdated);
            } catch (Exception e) {
                // log error message and move on to next iteration
                LOG.error(e.getMessage());
            }

            // sleep for the configured timeout
            long sleepTime = this.conf.getLong(
                    "netflix.genie.server.janitor.sleep.ms", 5000);
            LOG.info("Job janitor daemon going to sleep");
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStop(final boolean stop) {
        this.stop = stop;
    }
}
