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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.common.model.Types.SubprocessStatus;
import com.netflix.genie.server.persistence.PersistenceManager;
import java.util.Date;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Janitor thread that marks jobs as zombies if status hasn't been updated for
 * the configured timeout.
 *
 * @author skrishnan
 * @author tgianos
 */
public class JobJanitor extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(JobJanitor.class);

    private final PersistenceManager<Job> pm;
    private final AbstractConfiguration conf;
    private boolean stop;

    /**
     * Default constructor - initializes members correctly in order.
     */
    public JobJanitor() {
        conf = ConfigurationManager.getConfigInstance();
        pm = new PersistenceManager<Job>();
        stop = false;
    }

    /**
     * Mark jobs as zombies if status hasn't been updated for
     * netflix.genie.server.janitor.zombie.delta.ms.
     *
     * @return Number of jobs marked as zombies
     * @throws Exception if there is any error during the process
     */
    public int markZombies() throws Exception {
        // the equivalent query is as follows:
        // update Job set status='FAILED', finishTime=$max, exitCode=$zombie_code, 
        // statusMsg='Job has been marked as a zombie'
        // where updateTime < $min and (status='RUNNING' or status='INIT')"
        long currentTime = System.currentTimeMillis();
        long zombieTime = conf.getLong(
                "netflix.genie.server.janitor.zombie.delta.ms", 1800000);

        final StringBuilder builder = new StringBuilder();
        builder.append("UPDATE Job j ");
        builder.append("SET j.status = :failed, j.finishTime = :currentTime, j.exitCode = :exitCode, j.statusMsg = :statusMessage ");
        builder.append("WHERE j.updated < :updated AND (j.status = :running OR j.status = :init)");
        final EntityManager em = pm.createEntityManager();
        try {
            final Query query = em.createQuery(builder.toString())
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
            if (stop) {
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
            long sleepTime = conf.getLong(
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
     * Tell the janitor thread to stop running at next iteration.
     *
     * @param stop true if the thread should stop running
     */
    public void setStop(boolean stop) {
        this.stop = stop;
    }
}
