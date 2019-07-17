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

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.apis.rest.v3.controllers.ApplicationRestController;
import com.netflix.genie.web.apis.rest.v3.hateoas.resources.ApplicationResource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;

/**
 * Assembles Application resources out of applications.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class ApplicationResourceAssembler implements ResourceAssembler<Application, ApplicationResource> {

    private static final String COMMANDS_LINK = "commands";

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationResource toResource(final Application application) {
        final String id = application.getId().orElseThrow(IllegalArgumentException::new);
        final ApplicationResource applicationResource = new ApplicationResource(application);

        try {
            applicationResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(ApplicationRestController.class)
                        .getApplication(id)
                ).withSelfRel()
            );

            applicationResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(ApplicationRestController.class)
                        .getCommandsForApplication(id, null)
                ).withRel(COMMANDS_LINK)
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return applicationResource;
    }
}
