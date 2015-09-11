/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.startup;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.metrics.JobCountMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * This class bootstraps some common Genie spring stuff.
 *
 * @author tgianos
 */
@Component
public class GenieSpringBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(GenieSpringBootstrap.class);

    private final JobJanitor jobJanitor;
    private final JobCountMonitor jobCountMonitor;

    private final Thread jobCountMonitorThread;
    private final Thread jobJanitorThread;

    /**
     * Constructor.
     *
     * @param janitor         The job janitor to used
     * @param jobCountMonitor The job count monitor to use
     */
    @Autowired
    public GenieSpringBootstrap(
            final JobJanitor janitor,
            final JobCountMonitor jobCountMonitor
    ) {
        this.jobJanitor = janitor;
        this.jobCountMonitor = jobCountMonitor;
        this.jobCountMonitorThread = new Thread(this.jobCountMonitor);
        this.jobJanitorThread = new Thread(this.jobJanitor);
    }

    /**
     * Initialize the application here - db connections, daemon threads, etc.
     *
     * @throws GenieException if there is an error
     */
    @PostConstruct
    public void initialize() throws GenieException {
        LOG.info("called");

        // initialize and start the job janitor
        this.jobJanitorThread.setName("Job Janitor");
        this.jobJanitorThread.setDaemon(true);
        this.jobJanitorThread.start();

        // initialize and start the job monitor
        this.jobCountMonitorThread.setName("Job Count Monitor");
        this.jobCountMonitorThread.setDaemon(true);
        this.jobCountMonitorThread.start();
    }

    /**
     * Cleanup/shutdown when context is destroyed.
     */
    @PreDestroy
    public void shutdown() {
        LOG.info("called");

        // shut down dependencies cleanly
        this.jobJanitor.setStop(true);
        this.jobCountMonitor.setStop(true);

        LOG.info("done");
    }
}
