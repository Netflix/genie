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
import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.services.ExecutionService;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

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

    private final ExecutionService xs;

    /**
     * Default constructor - initializes members correctly in order.
     *
     * @param xs The execution service to use.
     */
    @Inject
    public JobJanitorImpl(final ExecutionService xs) {
        this.xs = xs;
        this.conf = ConfigurationManager.getConfigInstance();
        this.stop = false;
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     */
    @Override
    public int markZombies() throws Exception {
        return this.xs.markZombies();
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
                LOG.error(e.getMessage(), e);
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
