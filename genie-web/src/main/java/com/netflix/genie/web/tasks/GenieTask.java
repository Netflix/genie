/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.tasks;

import org.springframework.scheduling.Trigger;

/**
 * Interface for any task that should run in the Genie system. Will be extended by more specific interfaces.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface GenieTask extends Runnable {

    /**
     * Get the type of scheduling mechanism which should be used to schedule this task.
     *
     * @return The schedule type
     */
    GenieTaskScheduleType getScheduleType();

    /**
     * Get the Trigger which this task should be scheduled with.
     *
     * @return The trigger
     */
    Trigger getTrigger();

    /**
     * Get how long the system should wait between invoking the run() method of this task in milliseconds.
     *
     * @return The period to wait between invocations of run for this task
     */
    long getFixedRate();

    /**
     * Get how long the system should wait between invoking the run() method of this task in milliseconds after the
     * last successful run of the task.
     *
     * @return The time to delay between task completions
     */
    long getFixedDelay();
}
