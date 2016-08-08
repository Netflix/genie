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

/**
 * An enumeration representing the various reasons a JobFinishedEvent would be sent.
 *
 * @author tgianos
 * @since 3.0.0
 */
public enum JobFinishedReason {
    /**
     * The job was killed.
     */
    KILLED,

    /**
     * The job request was invalid.
     */
    INVALID,

    /**
     * The job failed during the initialization phase.
     */
    FAILED_TO_INIT,

    /**
     * The jobs process completed either sucessfully or unsuccessfully. Check job done file.
     */
    PROCESS_COMPLETED,

    /**
     * System crash during initialization.
     */
    SYSTEM_CRASH
}
