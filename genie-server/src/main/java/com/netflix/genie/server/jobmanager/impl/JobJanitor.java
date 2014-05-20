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

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;

/**
 * Janitor thread that marks jobs as zombies if status hasn't been updated for
 * the configured timeout.
 *
 * @author skrishnan
 */
public class JobJanitor extends Thread {
    private static Logger logger = LoggerFactory.getLogger(JobJanitor.class);

    private PersistenceManager<JobElement> pm;
    private AbstractConfiguration conf;
    private boolean stop;

    /**
     * Default constructor - initializes members correctly in order.
     */
    public JobJanitor() {
        conf = ConfigurationManager.getConfigInstance();
        pm = new PersistenceManager<JobElement>();
        stop = false;
    }

    /**
     * Mark jobs as zombies if status hasn't been updated for
     * netflix.genie.server.janitor.zombie.delta.ms.
     *
     * @return Number of jobs marked as zombies
     * @throws Exception
     *             if there is any error during the process
     */
    public int markZombies() throws Exception {
        // the equivalent query is as follows:
        // update JobElement set status='FAILED', updateTime=$max,
        // finishTime=$max,
        // exitCode=$zombie_code, statusMsg='Job has been marked as a zombie'
        // where updateTime < $min and (status='RUNNING' or status='INIT')"
        long currentTime = System.currentTimeMillis();
        long zombieTime = conf.getLong(
                "netflix.genie.server.janitor.zombie.delta.ms", 1800000);
        ClauseBuilder setCriteria = new ClauseBuilder(ClauseBuilder.COMMA);
        setCriteria.append("status='FAILED'");
        setCriteria.append("updateTime=" + currentTime);
        setCriteria.append("finishTime=" + currentTime);
        int exitCode = Types.SubprocessStatus.ZOMBIE_JOB.code();
        setCriteria.append("exitCode=" + exitCode);
        setCriteria.append("statusMsg='"
                + Types.SubprocessStatus.message(exitCode) + "'");

        // generate the where clause
        ClauseBuilder queryCriteria = new ClauseBuilder(ClauseBuilder.AND);
        queryCriteria.append("updateTime < " + (currentTime - zombieTime));
        ClauseBuilder statusCriteria = new ClauseBuilder(ClauseBuilder.OR);
        statusCriteria.append("status='RUNNING'");
        statusCriteria.append("status='INIT'");
        queryCriteria.append("(" + statusCriteria.toString() + ")", false);

        // set up the query
        QueryBuilder query = new QueryBuilder().table("JobElement")
                .clause(queryCriteria.toString()).set(setCriteria.toString());
        int numRowsUpdated = pm.update(query);
        return numRowsUpdated;
    }

    /**
     * The main run method for this thread - it runs for ever until explicitly
     * shutdown.
     */
    @Override
    public void run() {
        while (true) {
            logger.info("Job janitor daemon waking up");
            if (stop) {
                logger.info("Job janitor stopping as per request");
                return;
            }

            try {
                int numRowsUpdated = markZombies();
                logger.info("Total jobs marked as zombies: " + numRowsUpdated);
            } catch (Exception e) {
                // log error message and move on to next iteration
                logger.error(e.getMessage());
            }

            // sleep for the configured timeout
            long sleepTime = conf.getLong(
                    "netflix.genie.server.janitor.sleep.ms", 5000);
            logger.info("Job janitor daemon going to sleep");
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                logger.warn(e.getMessage());
                continue;
            }
        }
    }

    /**
     * Tell the janitor thread to stop running at next iteration.
     *
     * @param stop
     *            true if the thread should stop running
     */
    public void setStop(boolean stop) {
        this.stop = stop;
    }
}
