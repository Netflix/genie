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
import javax.validation.constraints.Size;

/**
 * An event thrown when a job is completed.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public class JobFinishedEvent extends BaseJobEvent {

    private final JobFinishedReason reason;
    private final String message;

    /**
     * Constructor.
     *
     * @param id      The id of the job that just finished
     * @param reason  The reason this job has finished
     * @param message Any message for why the job completed which should be saved
     * @param source  The source which created the event.
     */
    public JobFinishedEvent(
        @NotEmpty final String id,
        @NotNull final JobFinishedReason reason,
        @NotEmpty @Size(max = 255, min = 1) final String message,
        @NotNull final Object source
    ) {
        super(id, source);
        this.reason = reason;
        this.message = message;
    }
}
