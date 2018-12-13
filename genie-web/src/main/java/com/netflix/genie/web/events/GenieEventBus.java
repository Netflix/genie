/*
 *
 *  Copyright 2017 Netflix, Inc.
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

import lombok.NonNull;
import org.springframework.context.ApplicationEvent;

/**
 * Genie Event Bus interface.
 *
 * @author tgianos
 * @since 3.1.2
 */
public interface GenieEventBus {

    /**
     * Publish an event in the same thread as the calling thread.
     *
     * @param event The event to publish
     */
    void publishSynchronousEvent(@NonNull ApplicationEvent event);

    /**
     * Publish an event in a different thread than the calling thread.
     *
     * @param event The event to publish
     */
    void publishAsynchronousEvent(@NonNull ApplicationEvent event);
}
