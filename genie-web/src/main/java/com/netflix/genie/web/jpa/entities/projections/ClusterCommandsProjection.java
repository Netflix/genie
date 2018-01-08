/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities.projections;

import com.netflix.genie.web.jpa.entities.CommandEntity;

import java.util.List;

/**
 * Projection to return only the commands for a cluster.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface ClusterCommandsProjection {

    /**
     * Get the commands associated with a cluster.
     *
     * @return The list of commands in priority order
     */
    List<CommandEntity> getCommands();
}
