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
package com.netflix.genie.core.events;

import com.netflix.genie.common.dto.JobExecution;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * An event fired when a new job is started on a node.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public class JobStartedEvent extends ApplicationEvent {

    private static final long serialVersionUID = -8417875991681866646L;

    private final JobExecution jobExecution;

    /**
     * Constructor.
     *
     * @param jobExecution The job execution information for the job that was started.
     * @param source       The source which threw this event
     */
    public JobStartedEvent(@NotNull @Valid final JobExecution jobExecution, @NotNull final Object source) {
        super(source);
        this.jobExecution = jobExecution;
    }
}
