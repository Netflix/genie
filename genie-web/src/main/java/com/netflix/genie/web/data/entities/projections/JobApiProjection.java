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
package com.netflix.genie.web.data.entities.projections;

/**
 * A projection which allows the system to pull back whether the job was submitted via the REST API or other mechanism.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface JobApiProjection {

    /**
     * Return whether or not this job was submitted via the API or other (e.g. Agent CLI) mechanism.
     *
     * @return {@literal true} if the job was submitted via an API call. {@literal false} otherwise.
     */
    boolean isApi();
}
