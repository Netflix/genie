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
package com.netflix.genie.server.startup.impl;

import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.metrics.JobCountMonitor;
import com.netflix.genie.server.startup.GenieApplication;
import com.netflix.karyon.spi.Application;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides custom initialization for Genie during startup.
 *
 * @author skrishnan
 * @author tgianos
 */
@Application
@Named
public class GenieApplicationImpl implements GenieApplication {

    private static final Logger LOG = LoggerFactory.getLogger(GenieApplicationImpl.class);

    private final JobJanitor jobJanitor;
    private final GenieNodeStatistics genieNodeStatistics;
    private final JobCountManager jobCountManager;
    private final JobCountMonitor jobCountMonitor;
    
    private final Thread jobCountMonitorThread;
    private final Thread jobJanitorThread;

    @Inject
    public GenieApplicationImpl(
            final JobJanitor janitor,
            final GenieNodeStatistics genieNodeStatistics,
            final JobCountManager jobCountManager,
            final JobCountMonitor jobCountMonitor) {
        this.jobJanitor = janitor;
        this.genieNodeStatistics = genieNodeStatistics;
        this.jobCountManager = jobCountManager;
        this.jobCountMonitor = jobCountMonitor;
        this.jobCountMonitorThread = new Thread(this.jobCountMonitor);
        this.jobJanitorThread = new Thread(this.jobJanitor);
    }

    /**
     * {@inheritDoc}
     * @throws Exception 
     */
    @PostConstruct
    @Override
    public void initialize() throws Exception {
        LOG.info("called");

        // hack to ensure that a DB connection is made correctly for the first time
        // work-around for: https://issues.apache.org/jira/browse/OPENJPA-2139
//        this.jobCountManager.getNumInstanceJobs();
//        LOG.info("JobCountManager has been initialized successfully");

        // register the servo metrics
        this.genieNodeStatistics.register();
        LOG.info("Custom servo metrics have been registered");

        // initialize and start the job janitor
        this.jobJanitorThread.setDaemon(true);
        this.jobJanitorThread.start();
        
        // initialize and start the job monitor
        this.jobCountMonitorThread.setDaemon(true);
        this.jobCountMonitorThread.start();
    }

    /**
     * {@inheritDoc}
     */
    @PreDestroy
    @Override
    public void shutdown() {
        LOG.info("called");

        // shut down dependencies cleanly
        this.jobJanitor.setStop(true);
        this.jobCountMonitor.setStop(true);
        this.genieNodeStatistics.shutdown();

        LOG.info("done");
    }
}
