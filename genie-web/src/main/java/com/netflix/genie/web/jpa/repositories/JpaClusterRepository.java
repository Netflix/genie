/*
 * Copyright 2015 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package com.netflix.genie.web.jpa.repositories;

import com.netflix.genie.web.jpa.entities.ClusterEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.Set;

/**
 * Cluster repository.
 *
 * @author tgianos
 */
public interface JpaClusterRepository extends JpaBaseRepository<ClusterEntity>, CriteriaResolutionRepository {

    /**
     * The SQL to find all clusters in a TERMINATED state that aren't attached to any jobs still in the database.
     */
    String FIND_TERMINATED_CLUSTERS_SQL =
        "SELECT id "
            + "FROM clusters "
            + "WHERE id NOT IN (SELECT DISTINCT(cluster_id) FROM jobs WHERE cluster_id IS NOT NULL) "
            + "AND status = 'TERMINATED' "
            + "FOR UPDATE;";

    /**
     * Find the ids of all clusters that are in a terminated state and aren't attached to any jobs.
     *
     * @return The IDs of matching clusters
     */
    @Query(value = FIND_TERMINATED_CLUSTERS_SQL, nativeQuery = true)
    Set<Number> findTerminatedUnusedClusters();

    /**
     * Delete all clusters whose ids are contained in the given set of ids.
     *
     * @param ids The ids of the clusters to delete
     * @return The number of deleted clusters
     */
    @Modifying
    Long deleteByIdIn(final Set<Long> ids);
}
