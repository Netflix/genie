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
package com.netflix.genie.web.apis.rest.v3.hateoas.assemblers;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.apis.rest.v3.controllers.JobRestController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;

import javax.annotation.Nonnull;

/**
 * Assembles Job resources out of job DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobModelAssembler implements RepresentationModelAssembler<Job, EntityModel<Job>> {

    private static final String REQUEST_LINK = "request";
    private static final String EXECUTION_LINK = "execution";
    private static final String OUTPUT_LINK = "output";
    private static final String STATUS_LINK = "status";
    private static final String METADATA_LINK = "metadata";
    private static final String CLUSTER_LINK = "cluster";
    private static final String COMMAND_LINK = "command";
    private static final String APPLICATIONS_LINK = "applications";

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public EntityModel<Job> toModel(final Job job) {
        final String id = job.getId().orElseThrow(IllegalArgumentException::new);
        final EntityModel<Job> jobModel = new EntityModel<>(job);

        try {
            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJob(id)
                ).withSelfRel()
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

            jobModel.add(
                WebMvcLinkBuilder
                    .linkTo(JobRestController.class)
                    .slash(id)
                    .slash(OUTPUT_LINK)
                    .withRel(OUTPUT_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobRequest(id)
                ).withRel(REQUEST_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobExecution(id)
                ).withRel(EXECUTION_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobMetadata(id)
                ).withRel(METADATA_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobStatus(id)
                ).withRel(STATUS_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobCluster(id)
                ).withRel(CLUSTER_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobCommand(id)
                ).withRel(COMMAND_LINK)
            );

            jobModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(JobRestController.class)
                        .getJobApplications(id)
                ).withRel(APPLICATIONS_LINK)
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return jobModel;
    }
}
