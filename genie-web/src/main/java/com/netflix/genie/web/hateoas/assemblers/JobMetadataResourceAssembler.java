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
package com.netflix.genie.web.hateoas.assemblers;

import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.controllers.JobRestController;
import com.netflix.genie.web.hateoas.resources.JobMetadataResource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.stereotype.Component;

/**
 * Assembles Job Request resources out of JobRequest DTOs.
 *
 * @author tgianos
 * @since 3.3.5
 */
@Component
public class JobMetadataResourceAssembler implements ResourceAssembler<JobMetadata, JobMetadataResource> {

    /**
     * {@inheritDoc}
     */
    @Override
    public JobMetadataResource toResource(final JobMetadata jobMetadata) {
        final String id = jobMetadata.getId().orElseThrow(IllegalArgumentException::new);
        final JobMetadataResource jobMetadataResource = new JobMetadataResource(jobMetadata);

        try {
            jobMetadataResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobMetadata(id)
                ).withSelfRel()
            );

            jobMetadataResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJob(id)
                ).withRel("job")
            );

            jobMetadataResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobRequest(id)
                ).withRel("request")
            );

            final String output = "output";
            jobMetadataResource.add(
                ControllerLinkBuilder
                    .linkTo(JobRestController.class)
                    .slash(id)
                    .slash(output)
                    .withRel(output)
            );

            jobMetadataResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobStatus(id)
                ).withRel("status")
            );

            jobMetadataResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobExecution(id)
                ).withRel("execution")
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return jobMetadataResource;
    }
}
