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

import com.netflix.genie.common.messages.HiveConfigRequest;
import com.netflix.genie.common.messages.HiveConfigResponse;

/**
 * Abstraction layer to encapsulate data HiveConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author skrishnan
 */
public interface HiveConfigService {

    /**
     * Get Hive config for given id.
     *
     * @param id
     *            unique id for hive config
     * @return successful response, or one with an HTTP error code
     */
    HiveConfigResponse getHiveConfig(String id);

    /**
     * Get Hive config for various params.
     *
     * @param id
     *            unique id for config (can be null)
     * @param name
     *            name for config (can be null)
     * @param type
     *            type for config (can be null)
     * @return successful response, or one with an HTTP error code
     */
    HiveConfigResponse getHiveConfig(String id, String name, String type);

    /**
     * Create new hive config.
     *
     * @param request
     *            contains the hive config element for creation
     * @return successful response, or one with an HTTP error code
     */
    HiveConfigResponse createHiveConfig(HiveConfigRequest request);

    /**
     * Insert/update hive config.
     *
     * @param request
     *            contains the hive config element for update, must contain
     *            valid id
     * @return successful response, or one with an HTTP error code
     */
    HiveConfigResponse updateHiveConfig(HiveConfigRequest request);

    /**
     * Delete a hive config from database.
     *
     * @param id
     *            unique id for config to delete
     * @return successful response, or one with an HTTP error code
     */
    HiveConfigResponse deleteHiveConfig(String id);
}
