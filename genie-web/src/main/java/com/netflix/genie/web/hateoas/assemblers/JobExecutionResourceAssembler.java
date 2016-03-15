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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.controllers.JobRestController;
import com.netflix.genie.web.hateoas.resources.JobExecutionResource;
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
public class JobExecutionResourceAssembler implements ResourceAssembler<JobExecution, JobExecutionResource> {

    /**
     * {@inheritDoc}
     */
    @Override
    public JobExecutionResource toResource(final JobExecution jobExecution) {
        final JobExecutionResource jobExecutionResource = new JobExecutionResource(jobExecution);

        try {
            jobExecutionResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobExecution(jobExecution.getId())
                ).withSelfRel()
            );

            jobExecutionResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJob(jobExecution.getId())
                ).withRel("job")
            );

            jobExecutionResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobRequest(jobExecution.getId())
                ).withRel("request")
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

            jobExecutionResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobStatus(jobExecution.getId())
                ).withRel("status")
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return jobExecutionResource;
    }
}
