/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.data.observers;

import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobStateChangeEvent;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

/**
 * Observer of persisted entities modifications that publishes events on the event bus to be consumed asynchronously by
 * interested consumers.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class PersistedJobStatusObserverImpl implements PersistedJobStatusObserver {
    private final GenieEventBus genieEventBus;

    /**
     * Constructor.
     *
     * @param genieEventBus the genie event bus
     */
    public PersistedJobStatusObserverImpl(
        final GenieEventBus genieEventBus
    ) {
        this.genieEventBus = genieEventBus;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notify(
        final String jobId,
        @Nullable final JobStatus previousStatus,
        final JobStatus currentStatus
    ) {
        final JobStateChangeEvent event = new JobStateChangeEvent(jobId, previousStatus, currentStatus, this);
        log.warn("Publishing event: {}", event);
        this.genieEventBus.publishAsynchronousEvent(event);
    }

}
