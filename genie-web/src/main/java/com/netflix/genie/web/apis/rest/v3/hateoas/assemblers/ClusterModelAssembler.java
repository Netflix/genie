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

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.apis.rest.v3.controllers.ClusterRestController;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.RepresentationModelAssembler;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;

import javax.annotation.Nonnull;

/**
 * Assembles Cluster resources out of clusters.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class ClusterModelAssembler implements RepresentationModelAssembler<Cluster, EntityModel<Cluster>> {

    private static final String COMMANDS_LINK = "commands";

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public EntityModel<Cluster> toModel(final Cluster cluster) {
        final String id = cluster.getId().orElseThrow(IllegalArgumentException::new);
        final EntityModel<Cluster> clusterModel = new EntityModel<>(cluster);

        try {
            clusterModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(ClusterRestController.class)
                        .getCluster(id)
                ).withSelfRel()
            );

            clusterModel.add(
                WebMvcLinkBuilder.linkTo(
                    WebMvcLinkBuilder
                        .methodOn(ClusterRestController.class)
                        .getCommandsForCluster(id, null)
                ).withRel(COMMANDS_LINK)
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return clusterModel;
    }
}
