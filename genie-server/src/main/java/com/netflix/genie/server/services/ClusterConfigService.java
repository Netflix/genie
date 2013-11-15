/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.server.services;

import java.util.List;

import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.Types;

/**
 * Abstraction layer to encapsulate data ClusterConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe
 *
 * @author skrishnan
 */
public interface ClusterConfigService {

    /**
     * Gets the cluster config by id.
     *
     * @param id
     *            unique id of cluster config to return
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String id);

    /**
     * Get the cluster config by various params.
     *
     * @param id
     *            unique id for cluster (can be a pattern)
     * @param name
     *            name of cluster (can be a pattern)
     * @param config
     *            configuration supported
     * @param schedule
     *            schedule supported
     * @param jobType
     *            job type supported
     * @param status
     *            status for cluster
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String id, String name,
            Types.Configuration config, Types.Schedule schedule,
            Types.JobType jobType, Types.ClusterStatus status);

    /**
     * Get cluster info for various params Null parameters are ignored.
     *
     * @param id
     *            unique id for cluster
     * @param name
     *            cluster name
     * @param prod
     *            if cluster supports prod jobs
     * @param test
     *            if cluster supports test jobs
     * @param unitTest
     *            if cluster supports unitTest (dev) jobs
     * @param adHoc
     *            if cluster supports ad-hoc jobs
     * @param sla
     *            if cluster supports sla jobs
     * @param bonus
     *            if cluster supports bonus jobs
     * @param status
     *            valid types - Types.ClusterStatus
     * @param hasStats
     *            whether the cluster is logging statistics or not
     * @param minUpdateTime
     *            min time when cluster config was updated
     * @param maxUpdateTime
     *            max time when cluster config was updated
     * @param limit
     *            number of entries to return
     * @param page
     *            page number
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String id, String name,
            Boolean prod, Boolean test, Boolean unitTest, Boolean adHoc,
            Boolean sla, Boolean bonus, String jobType, List<String> status,
            Boolean hasStats, Long minUpdateTime, Long maxUpdateTime,
            Integer limit, Integer page);

    /**
     * Create new cluster config.
     *
     * @param request
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse createClusterConfig(ClusterConfigRequest request);

    /**
     * Update/insert cluster config.
     *
     * @param request
     *            enscapsulates cluster config to upsert, must contain valid id
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse updateClusterConfig(ClusterConfigRequest request);

    /**
     * Delete a cluster config by id.
     *
     * @param id
     *            unique id for cluster to delete
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse deleteClusterConfig(String id);
}
