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
package com.netflix.genie.web.events;

import lombok.Getter;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.concurrent.Future;

/**
 * Event when a job is scheduled to be executed.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public class JobScheduledEvent extends BaseJobEvent {

    private Future<?> task;
    private int memory;

    /**
     * Constructor.
     *
     * @param id     The id of the job that was scheduled
     * @param task   The future representing the thread that will setup and run the job
     * @param memory The amount of memory (in MB) the job is scheduled to use
     * @param source The source object which generated this event
     */
    public JobScheduledEvent(
        @NotEmpty final String id,
        @NotNull final Future<?> task,
        final int memory,
        @NotNull final Object source
    ) {
        super(id, source);
        this.task = task;
        this.memory = memory;
    }
}
