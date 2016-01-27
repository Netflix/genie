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
package com.netflix.genie.web.tasks.impl;

import com.netflix.genie.web.tasks.JobJanitor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Janitor thread that marks jobs as zombies if status hasn't been updated for
 * the configured timeout.
 *
 * @author skrishnan
 * @author tgianos
 */
//TODO: Add conditionals to enable or disable
@Component
@Slf4j
public class JobJanitorImpl implements JobJanitor {

    //private final ExecutionService xs;
    @Value("${com.netflix.genie.server.janitor.min.sleep.ms:300000}")
    private long minSleepTime;
    @Value("${com.netflix.genie.server.janitor.max.sleep.ms:600000}")
    private long maxSleepTime;

    /**
     * Default constructor - initializes members correctly in order.
     */
    public JobJanitorImpl() {
        //this.xs = xs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Scheduled(fixedRate = 450000, initialDelay = 60000) // TODO: Randomize? Do we need this to be a property?
    public void markZombies() {
        log.info("Checking for zombies...");
        //final int zombies = this.xs.markZombies();
        final int zombies = 0;
        log.info("Marked " + zombies + " jobs as zombies");
    }
}
