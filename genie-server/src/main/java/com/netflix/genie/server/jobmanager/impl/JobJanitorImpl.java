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
package com.netflix.genie.server.jobmanager.impl;

import java.util.Random;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.services.ExecutionService;
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
    public JobJanitorImpl(final ExecutionService xs) {
        this.xs = xs;
        this.conf = ConfigurationManager.getConfigInstance();
        this.stop = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int markZombies() {
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

            // get min sleep time
            final long minSleepTime = conf.getLong(
                    "com.netflix.genie.server.janitor.min.sleep.ms", 300000);

            // get max sleep time
            final long maxSleepTime = conf.getLong(
                    "com.netflix.genie.server.janitor.max.sleep.ms", 600000);

            // calculate a random number of seconds between min and max to sleep.
            // This is done to stagger the janitor threads across multiple instances
            Random randomGenerator = new Random();
            // Since its a few seconds the long to int cast is fine
            long randomSleepTime = randomGenerator.nextInt((int) (maxSleepTime - minSleepTime)) + minSleepTime;

            LOG.info("Job janitor daemon going to sleep for " + randomSleepTime / 1000 + " seconds.");
            try {
                Thread.sleep(randomSleepTime);
            } catch (final InterruptedException e) {
                LOG.warn(e.getMessage(), e);
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
