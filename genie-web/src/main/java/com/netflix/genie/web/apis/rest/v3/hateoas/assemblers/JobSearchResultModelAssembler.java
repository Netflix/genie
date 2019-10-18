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
package com.netflix.genie.web.apis.rest.v3.hateoas.assemblers;

import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.apis.rest.v3.controllers.JobRestController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;

import javax.annotation.Nonnull;

/**
 * Assembles Job resources out of job search result DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobSearchResultModelAssembler
    implements RepresentationModelAssembler<JobSearchResult, EntityModel<JobSearchResult>> {

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public EntityModel<JobSearchResult> toModel(final JobSearchResult job) {
        final EntityModel<JobSearchResult> jobSearchResultModel = new EntityModel<>(job);

        try {
            jobSearchResultModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJob(job.getId())
                ).withSelfRel()
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return jobSearchResultModel;
    }
}
