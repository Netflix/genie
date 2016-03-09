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

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.controllers.ClusterRestController;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.stereotype.Component;

/**
 * Assembles Cluster resources out of clusters.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
public class ClusterResourceAssembler implements ResourceAssembler<Cluster, ClusterResource> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ClusterResource toResource(final Cluster cluster) {
        final ClusterResource clusterResource = new ClusterResource(cluster);

        try {
            clusterResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(ClusterRestController.class)
                        .getCluster(cluster.getId())
                ).withSelfRel()
            );

            clusterResource.add(
                ControllerLinkBuilder.linkTo(
                    ControllerLinkBuilder
                        .methodOn(ClusterRestController.class)
                        .getCommandsForCluster(cluster.getId(), null)
                ).withRel("commands")
            );
        } catch (final GenieException ge) {
            // If we can't convert it we might as well force a server exception
            throw new RuntimeException(ge);
        }

        return clusterResource;
    }
}
