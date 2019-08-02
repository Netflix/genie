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
package com.netflix.genie.web.services;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.dtos.CompletedJobSummary;

import javax.validation.constraints.NotBlank;

/**
 * Produces job summaries by collating information from various underlying data sources.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JobSummaryService {

    /**
     * Get completed job summary for given job id.
     *
     * @param id id of job to look up
     * @return the job summary
     * @throws GenieException if there is an error
     */
    CompletedJobSummary getJobSummary(@NotBlank String id) throws GenieException;
}
