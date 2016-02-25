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

import lombok.Getter;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.context.ApplicationEvent;

/**
 * An event fired from within the Genie system when it needs a specific job killed.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public class KillJobEvent extends ApplicationEvent {

    private static final long serialVersionUID = 1701855508124286343L;
    private final String id;

    /**
     * Constructor.
     *
     * @param id  The id of the job to kill
     * @param source The source object which threw this event
     */
    public KillJobEvent(@NotBlank final String id, final Object source) {
        super(source);
        this.id = id;
    }
}
