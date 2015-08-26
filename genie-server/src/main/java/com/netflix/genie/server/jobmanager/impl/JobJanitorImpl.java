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

import com.netflix.genie.server.jobmanager.JobJanitor;
import com.netflix.genie.server.services.ExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Janitor thread that marks jobs as zombies if status hasn't been updated for
 * the configured timeout.
 *
 * @author skrishnan
 * @author tgianos
 */
@Component
public class JobJanitorImpl implements JobJanitor {

    private static final Logger LOG = LoggerFactory.getLogger(JobJanitorImpl.class);
    private final ExecutionService xs;
    @Value("${com.netflix.genie.server.janitor.min.sleep.ms:300000}")
    private long minSleepTime;
    @Value("${com.netflix.genie.server.janitor.max.sleep.ms:600000}")
    private long maxSleepTime;
    private boolean stop;

    /**
     * Default constructor - initializes members correctly in order.
     *
     * @param xs The execution service to use.
     */
    @Autowired
    public JobJanitorImpl(final ExecutionService xs) {
        this.xs = xs;
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
                final int numRowsUpdated = markZombies();
                LOG.info("Total jobs marked as zombies: " + numRowsUpdated);
            } catch (Exception e) {
                // log error message and move on to next iteration
                LOG.error(e.getMessage(), e);
            }

            // calculate a random number of seconds between min and max to sleep.
            // This is done to stagger the janitor threads across multiple instances
            final Random randomGenerator = new Random();
            // Since its a few seconds the long to int cast is fine
            final long randomSleepTime =
                    randomGenerator.nextInt((int) (this.maxSleepTime - this.minSleepTime)) + this.minSleepTime;

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
