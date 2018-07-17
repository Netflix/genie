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
import org.springframework.context.ApplicationEvent;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

/**
 * A base event all Genie job events should extend.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
public class BaseJobEvent extends ApplicationEvent {

    private static final long serialVersionUID = 8635146103611268924L;

    private String id;

    /**
     * Constructor.
     *
     * @param id     The id of the job this event relates to
     * @param source The source object which generates this event
     */
    public BaseJobEvent(@NotEmpty final String id, @NotNull final Object source) {
        super(source);
        this.id = id;
    }
}
