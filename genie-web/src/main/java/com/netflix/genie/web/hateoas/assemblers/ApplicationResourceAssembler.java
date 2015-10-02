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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.controllers.ApplicationRestController;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.hateoas.mvc.ResourceAssemblerSupport;
import org.springframework.stereotype.Component;

/**
 * Assembles Application resources out of applications.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
public class ApplicationResourceAssembler extends ResourceAssemblerSupport<Application, ApplicationResource> {

    /**
     * Default constructor.
     */
    public ApplicationResourceAssembler() {
        super(ApplicationRestController.class, ApplicationResource.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationResource toResource(final Application application) {
        final ApplicationResource applicationResource = new ApplicationResource(application);

        try {
            applicationResource.add(
                    ControllerLinkBuilder.linkTo(
                            ControllerLinkBuilder.methodOn(ApplicationRestController.class)
                                    .getApplication(application.getId()))
                            .withSelfRel()
            );

            applicationResource.add(
                    ControllerLinkBuilder.linkTo(
                            ControllerLinkBuilder.methodOn(ApplicationRestController.class)
                                    .getCommandsForApplication(
                                            application.getId(),
                                            Sets.newHashSet(
                                                    CommandStatus.ACTIVE.toString(),
                                                    CommandStatus.DEPRECATED.toString(),
                                                    CommandStatus.INACTIVE.toString()
                                            )
                                    )
                    ).withRel("commands")
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return applicationResource;
    }
}
