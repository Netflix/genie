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

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.apis.rest.v3.controllers.ApplicationRestController;
import com.netflix.genie.web.apis.rest.v3.controllers.ClusterRestController;
import com.netflix.genie.web.apis.rest.v3.controllers.CommandRestController;
import com.netflix.genie.web.apis.rest.v3.controllers.JobRestController;
import com.netflix.genie.web.apis.rest.v3.controllers.RootRestController;
import com.netflix.genie.web.apis.rest.v3.hateoas.resources.RootResource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

/**
 * Assembles root resource from a JsonNode.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class RootResourceAssembler implements ResourceAssembler<JsonNode, RootResource> {

    private static final String APPLICATIONS_LINK = "applications";
    private static final String COMMANDS_LINK = "commands";
    private static final String CLUSTERS_LINK = "clusters";
    private static final String JOBS_LINK = "jobs";

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressFBWarnings("NP_NULL_PARAM_DEREF_ALL_TARGETS_DANGEROUS")
    public RootResource toResource(final JsonNode metadata) {
        final RootResource rootResource = new RootResource(metadata);

        try {
            rootResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(RootRestController.class)
                        .getRoot()
                ).withSelfRel()
            );

            rootResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(ApplicationRestController.class)
                        .createApplication(null)
                ).withRel(APPLICATIONS_LINK)
            );

            rootResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(CommandRestController.class)
                        .createCommand(null)
                ).withRel(COMMANDS_LINK)
            );

            rootResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(ClusterRestController.class)
                        .createCluster(null)
                ).withRel(CLUSTERS_LINK)
            );

            rootResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(JobRestController.class)
                        .submitJob(null, null, null, null)
                ).withRel(JOBS_LINK)
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return rootResource;
    }
}
