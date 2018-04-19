/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.common.internal.dto.v4;

import java.util.Optional;

/**
 * Common fields for Resource requests (clusters, commands, jobs, etc).
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface CommonRequest {

    /**
     * Get the ID the user has requested for this resource if one was added.
     *
     * @return The ID wrapped in an {@link Optional}
     */
    Optional<String> getRequestedId();

    /**
     * Get the resources the user requested for the job during execution if any.
     *
     * @return The execution resources
     */
    ExecutionEnvironment getResources();
}
