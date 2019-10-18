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

import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.apis.rest.v3.controllers.CommandRestController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;

import javax.annotation.Nonnull;

/**
 * Assembles Command resources out of commands.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class CommandModelAssembler implements RepresentationModelAssembler<Command, EntityModel<Command>> {

    private static final String APPLICATIONS_LINK = "applications";
    private static final String CLUSTERS_LINK = "clusters";

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public EntityModel<Command> toModel(final Command command) {
        final String id = command.getId().orElseThrow(IllegalArgumentException::new);
        final EntityModel<Command> commandModel = new EntityModel<>(command);

        try {
            commandModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(CommandRestController.class)
                        .getCommand(id)
                ).withSelfRel()
            );

            commandModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(CommandRestController.class)
                        .getApplicationsForCommand(id)
                ).withRel(APPLICATIONS_LINK)
            );

            commandModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(CommandRestController.class)
                        .getClustersForCommand(id, null)
                ).withRel(CLUSTERS_LINK)
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return commandModel;
    }
}
