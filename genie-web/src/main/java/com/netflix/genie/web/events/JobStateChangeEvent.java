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
package com.netflix.genie.web.events;

import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.Getter;
import lombok.ToString;
import org.springframework.context.ApplicationEvent;

import javax.annotation.Nullable;

/**
 * Event representing a job status change.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@ToString
public class JobStateChangeEvent extends ApplicationEvent {
    private final String jobId;
    private final JobStatus previousStatus;
    private final JobStatus newStatus;

    /**
     * Constructor.
     *
     * @param jobId          the job id
     * @param previousStatus the previous status, or null if the job was just created
     * @param newStatus      the status the job just transitioned to
     * @param source         the event source
     */
    public JobStateChangeEvent(
        final String jobId,
        @Nullable final JobStatus previousStatus,
        final JobStatus newStatus,
        final Object source
    ) {
        super(source);
        this.jobId = jobId;
        this.previousStatus = previousStatus;
        this.newStatus = newStatus;
    }
}
