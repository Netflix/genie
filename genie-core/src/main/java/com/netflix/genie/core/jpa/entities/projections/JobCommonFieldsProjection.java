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
package com.netflix.genie.core.jpa.entities.projections;

import com.netflix.genie.core.jpa.entities.TagEntity;

import java.util.Optional;
import java.util.Set;

/**
 * Projection for common fields between pre 3.3.0 JobRequest and Job entities.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JobCommonFieldsProjection extends BaseProjection {
    /**
     * Get the command arguments for this job.
     *
     * @return The command arguments or Optional of null
     */
    Optional<String> getCommandArgs();

    /**
     * Get the tags for the job.
     *
     * @return Any tags that were sent in when job was originally requested
     */
    Set<TagEntity> getTags();

    /**
     * Get the grouping this job is a part of. e.g. scheduler job name for job run many times
     *
     * @return The grouping
     */
    Optional<String> getGrouping();

    /**
     * Get the instance identifier of a grouping. e.g. the run id of a given scheduled job
     *
     * @return The grouping instance
     */
    Optional<String> getGroupingInstance();
}
