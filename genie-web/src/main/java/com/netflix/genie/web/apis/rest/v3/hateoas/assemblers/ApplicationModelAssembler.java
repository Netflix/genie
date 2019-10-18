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
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;

import javax.annotation.Nonnull;

/**
 * Assembles Application resources out of applications.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class ApplicationModelAssembler implements RepresentationModelAssembler<Application, EntityModel<Application>> {

    private static final String COMMANDS_LINK = "commands";

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public EntityModel<Application> toModel(final Application application) {
        final String id = application.getId().orElseThrow(IllegalArgumentException::new);
        final EntityModel<Application> applicationModel = new EntityModel<>(application);

        try {
            applicationModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(ApplicationRestController.class)
                        .getApplication(id)
                ).withSelfRel()
            );

            applicationModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(ApplicationRestController.class)
                        .getCommandsForApplication(id, null)
                ).withRel(COMMANDS_LINK)
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return applicationModel;
    }
}
