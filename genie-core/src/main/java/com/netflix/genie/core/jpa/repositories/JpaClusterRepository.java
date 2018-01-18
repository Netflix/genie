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
package com.netflix.genie.core.jpa.repositories;

import com.netflix.genie.core.jpa.entities.ClusterEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Set;

/**
 * Cluster repository.
 *
 * @author tgianos
 */
public interface JpaClusterRepository extends JpaBaseRepository<ClusterEntity> {
    /**
     * This is the query used to find clusters and commands for given criteria from a user.
     */
    String CLUSTER_COMMAND_QUERY =
        "SELECT "
            + "  cc.cluster_id,"
            + "  c.unique_id "
            + "FROM"
            + "  ("
            + "    SELECT"
            + "      cc.cluster_id as cluster_id,"
            + "      MIN(cc.command_order) as command_order"
            + "    FROM"
            + "      ("
            + "        SELECT"
            + "          c.id as id"
            + "        FROM"
            + "          clusters c join"
            + "          clusters_tags ct on c.id = ct.cluster_id join"
            + "          tags t on ct.tag_id = t.id"
            + "        WHERE"
            + "          t.tag IN (:clusterTags) AND"
            + "          c.status = 'UP'"
            + "        GROUP BY"
            + "          c.id"
            + "        HAVING"
            + "          COUNT(c.id) = :clusterTagsCount"
            + "      ) AS selected_clusters join"
            + "      clusters_commands cc ON selected_clusters.id = cc.cluster_id join"
            + "      ("
            + "        SELECT"
            + "          c.id as id"
            + "        FROM"
            + "          commands c join"
            + "          commands_tags ct on c.id = ct.command_id join"
            + "          tags t on ct.tag_id = t.id"
            + "        WHERE"
            + "          t.tag IN (:commandTags) AND"
            + "          c.status = 'ACTIVE'"
            + "        GROUP BY"
            + "          c.id"
            + "        HAVING"
            + "          COUNT(c.id) = :commandTagsCount"
            + "      ) AS selected_commands ON selected_commands.id = cc.command_id"
            + "    GROUP BY cc.cluster_id"
            + "  ) as cluster_id_order join"
            + "  clusters_commands cc on"
            + "    cluster_id_order.cluster_id = cc.cluster_id AND"
            + "    cc.command_order = cluster_id_order.command_order join"
            + "  commands c on cc.command_id = c.id;";

    /**
     * The SQL to find all clusters in a TERMINATED state that aren't attached to any jobs still in the database.
     */
    String DELETE_TERMINATED_CLUSTERS_SQL =
        "DELETE "
            + "FROM clusters "
            + "WHERE id NOT IN (SELECT DISTINCT(cluster_id) FROM jobs WHERE cluster_id IS NOT NULL) "
            + "AND status = 'TERMINATED';";

    /**
     * For referential integrity before clusters are deleted remove all tag references.
     */
    // TODO: Check when database scripts are used with proper cascade statements that this isn't needed. e.g. 4.0.0
    String DELETE_TERMINATED_CLUSTERS_TAGS_REFERENCES_SQL =
        "DELETE "
            + "FROM clusters_tags "
            + "WHERE cluster_id IN ("
            + " SELECT id "
            + " FROM clusters "
            + " WHERE id NOT IN (SELECT DISTINCT(cluster_id) FROM jobs WHERE cluster_id IS NOT NULL) "
            + " AND status = 'TERMINATED'"
            + ");";

    /**
     * For referential integrity before clusters are deleted remove all config file references.
     */
    // TODO: Check when database scripts are used with proper cascade statements that this isn't needed. e.g. 4.0.0
    String DELETE_TERMINATED_CLUSTERS_CONFIGS_FILE_REFERENCES_SQL =
        "DELETE "
            + "FROM clusters_configs "
            + "WHERE cluster_id IN ("
            + " SELECT id "
            + " FROM clusters "
            + " WHERE id NOT IN (SELECT DISTINCT(cluster_id) FROM jobs WHERE cluster_id IS NOT NULL) "
            + " AND status = 'TERMINATED'"
            + ");";

    /**
     * For referential integrity before clusters are deleted remove all dependency file references.
     */
    // TODO: Check when database scripts are used with proper cascade statements that this isn't needed. e.g. 4.0.0
    String DELETE_TERMINATED_CLUSTERS_DEPENDENCIES_FILE_REFERENCES_SQL =
        "DELETE "
            + "FROM clusters_dependencies "
            + "WHERE cluster_id IN ("
            + " SELECT id "
            + " FROM clusters "
            + " WHERE id NOT IN (SELECT DISTINCT(cluster_id) FROM jobs WHERE cluster_id IS NOT NULL) "
            + " AND status = 'TERMINATED'"
            + ");";

    /**
     * For referential integrity before clusters are deleted remove all command links.
     */
    // TODO: Check when database scripts are used with proper cascade statements that this isn't needed. e.g. 4.0.0
    String DELETE_TERMINATED_CLUSTERS_COMMANDS_REFERENCES_SQL =
        "DELETE "
            + "FROM clusters_commands "
            + "WHERE cluster_id IN ("
            + " SELECT id "
            + " FROM clusters "
            + " WHERE id NOT IN (SELECT DISTINCT(cluster_id) FROM jobs WHERE cluster_id IS NOT NULL) "
            + " AND status = 'TERMINATED'"
            + ");";

    /**
     * Find the cluster and command ids for the given criterion tags. The data is returned as a list of
     * Long, Long results where the first Long is the cluster id and the second is the command id that should be used
     * if that cluster is selected. The command id should be the highest priority command that matches the command
     * criterion which has been associated with the given cluster by administrators.
     *
     * @param clusterTags      The tags the cluster must match to be selected (conjunction). e.g. 'tag1','tag2','tag3'
     * @param clusterTagsCount The number of cluster tags (size of clusterTags)
     * @param commandTags      The tags the command associated with the cluster must match to be selected. (conjunction)
     *                         e.g. 'tag1','tag2'
     * @param commandTagsSize  The number of command tags (size of commandTags)
     * @return The id pairs found or empty list if no matches
     */
    @Query(value = CLUSTER_COMMAND_QUERY, nativeQuery = true)
    List<Object[]> findClustersAndCommandsForCriterion(
        @Param("clusterTags") final Set<String> clusterTags,
        @Param("clusterTagsCount") final int clusterTagsCount,
        @Param("commandTags") final Set<String> commandTags,
        @Param("commandTagsCount") final int commandTagsSize
    );

    /**
     * Delete all clusters that are in a terminated state and aren't attached to any jobs.
     *
     * @return The number of clusters deleted
     */
    @Modifying
    @Query(value = DELETE_TERMINATED_CLUSTERS_SQL, nativeQuery = true)
    int deleteTerminatedClusters();

    /**
     * Delete all links to clusters that are about to be terminated from commands.
     *
     * @return The number of records deleted.
     */
    @Modifying
    @Query(value = DELETE_TERMINATED_CLUSTERS_COMMANDS_REFERENCES_SQL, nativeQuery = true)
    int deleteTerminatedClustersCommandsReferences();

    /**
     * Delete all links to clusters that are about to be terminated from config file references.
     *
     * @return The number of records deleted.
     */
    @Modifying
    @Query(value = DELETE_TERMINATED_CLUSTERS_CONFIGS_FILE_REFERENCES_SQL, nativeQuery = true)
    int deleteTerminatedClustersConfigsFileReferences();

    /**
     * Delete all links to clusters that are about to be terminated from dependency file references.
     *
     * @return The number of records deleted.
     */
    @Modifying
    @Query(value = DELETE_TERMINATED_CLUSTERS_DEPENDENCIES_FILE_REFERENCES_SQL, nativeQuery = true)
    int deleteTerminatedClustersDependenciesFileReferences();

    /**
     * Delete all links to clusters that are about to be terminated from tags.
     *
     * @return The number of records deleted.
     */
    @Modifying
    @Query(value = DELETE_TERMINATED_CLUSTERS_TAGS_REFERENCES_SQL, nativeQuery = true)
    int deleteTerminatedClustersTagsReferences();
}
