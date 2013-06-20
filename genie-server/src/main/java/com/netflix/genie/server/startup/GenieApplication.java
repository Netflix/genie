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

package com.netflix.genie.server.startup;

import com.netflix.karyon.spi.Application;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.server.jobmanager.impl.JobJanitor;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.persistence.PersistenceManager;

/**
 * This class provides custom initialization for Genie during startup.
 *
 * @author skrishnan
 */
@Application
public class GenieApplication {

    private static Logger logger = LoggerFactory.getLogger(GenieApplication.class);

    private JobJanitor janitor;

    /**
     * Initialize the application here - db connections, daemon threads, etc.
     * @throws Exception
     */
    @PostConstruct
    public void initialize() throws Exception {
        logger.info("called");

        // initialize the persistence manager, so the connection pool is set up
        PersistenceManager.init();
        logger.info("PersistenceManager has been initialized");

        // now initialize the job count manager, which needs the PersistenceManager to be initialized
        JobCountManager.init();
        // hack to ensure that a DB connection is made correctly for the first time
        // work-around for: https://issues.apache.org/jira/browse/OPENJPA-2139
        JobCountManager.getNumInstanceJobs();
        logger.info("JobCountManager has been initialized successfully");

        // for the epic plugin, initialize it explicitly
        GenieNodeStatistics.init();
        logger.info("Custom epic metrics have been registered");

        // initialize and start the job janitor
        janitor = new JobJanitor();
        janitor.setDaemon(true);
        janitor.start();
    }

    /**
     * Cleanup/shutdown when context is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("called");

        // shut down dependencies cleanly
        janitor.setStop(true);
        GenieNodeStatistics.getInstance().shutdown();
        PersistenceManager.shutdown();

        logger.info("done");
    }
}
