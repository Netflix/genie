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
package com.netflix.genie.web.hateoas.assemblers;

import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.controllers.JobRestController;
import com.netflix.genie.web.hateoas.resources.JobSearchResultResource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

/**
 * Assembles Job resources out of job search result DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobSearchResultResourceAssembler implements ResourceAssembler<JobSearchResult, JobSearchResultResource> {

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSearchResultResource toResource(final JobSearchResult job) {
        final JobSearchResultResource jobResource = new JobSearchResultResource(job);

        try {
            jobResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJob(job.getId())
                ).withSelfRel()
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return jobResource;
    }
}
