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

/**
 * The enumeration values which a {@link GenieTask} can be be scheduled with.
 *
 * @author tgianos
 * @since 3.0.0
 */
public enum GenieTaskScheduleType {
    /**
     * When you want your task scheduled using a {@link org.springframework.scheduling.Trigger}.
     */
    TRIGGER,

    /**
     * When you want your task scheduled using a fixed rate in milliseconds.
     */
    FIXED_RATE,

    /**
     * When you want your tasked scheduled at a fixed rate but held for a time after the last completion of the task.
     */
    FIXED_DELAY
}
