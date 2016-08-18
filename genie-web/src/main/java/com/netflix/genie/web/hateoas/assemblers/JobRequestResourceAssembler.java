/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.controllers.JobRestController;
import com.netflix.genie.web.hateoas.resources.JobRequestResource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.stereotype.Component;

/**
 * Assembles Job Request resources out of JobRequest DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
public class JobRequestResourceAssembler implements ResourceAssembler<JobRequest, JobRequestResource> {

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequestResource toResource(final JobRequest jobRequest) {
        final String id = jobRequest.getId().orElseThrow(IllegalArgumentException::new);
        final JobRequestResource jobRequestResource = new JobRequestResource(jobRequest);

        try {
            jobRequestResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobRequest(id)
                ).withSelfRel()
            );

            jobRequestResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJob(id)
                ).withRel("job")
            );

            jobRequestResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobExecution(id)
                ).withRel("execution")
            );

            // TODO: https://github.com/spring-projects/spring-hateoas/issues/186 should be fixed in .20 currently .19
//            jobResource.add(
//                ControllerLinkBuilder.linkTo(
//                    JobRestController.class,
//                    JobRestController.class.getMethod(
//                        "getJobOutput",
//                        String.class,
//                        String.class,
//                        HttpServletRequest.class,
//                        HttpServletResponse.class
//                    ),
//                    job.getId(),
//                    null,
//                    null,
//                    null
//                ).withRel("output")
//            );

            final String output = "output";
            jobRequestResource.add(
                ControllerLinkBuilder
                    .linkTo(JobRestController.class)
                    .slash(id)
                    .slash(output)
                    .withRel(output)
            );

            jobRequestResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobStatus(id)
                ).withRel("status")
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return jobRequestResource;
    }
}
