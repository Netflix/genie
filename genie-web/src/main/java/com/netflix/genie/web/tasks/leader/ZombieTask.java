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
package com.netflix.genie.web.tasks.leader;

import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.Trigger;
import org.springframework.stereotype.Component;

/**
 * Janitor thread that marks jobs as zombies if status hasn't been updated for
 * the configured timeout.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
@Slf4j
public class ZombieTask implements LeadershipTask {

    /**
     * Find and mark zombies every time this thread is invoked.
     */
    @Override
    public void run() {
        //TODO: Implement actual logic...
        log.info("Checking for zombies...");
        final int zombies = 0;
        log.info("Marked " + zombies + " jobs as zombies");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.FIXED_RATE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Trigger getTrigger() {
        throw new UnsupportedOperationException("This task should only be scheduled at a fixed rate.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFixedRate() {
        //TODO: Implement getting this value from a property...for now for testing just hard code
        return 45000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFixedDelay() {
        throw new UnsupportedOperationException("This task should only be scheduled at a fixed rate.");
    }
}
