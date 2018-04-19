/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.jpa.repositories;

import com.netflix.genie.common.internal.dto.v4.Criterion;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Custom extension interfaces for the {@link JpaClusterRepository} which require more hands on control rather than
 * generated code from Spring.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface CriteriaResolutionRepository {

    /**
     * Given a cluster and command criterion attempt to resolve the cluster and highest priority command that match
     * said criteria.
     *
     * @param clusterCriterion The criterion for selecting a cluster
     * @param commandCriterion The criterion for selecting a command attached to the selected cluster
     * @return A tuple of the id of the cluster and the id of the command to use if that cluster is selected by the LB
     */
    //TODO: add algorithm explanation
    @Nonnull
    List<Object[]> resolveClustersAndCommands(final Criterion clusterCriterion, final Criterion commandCriterion);
}
