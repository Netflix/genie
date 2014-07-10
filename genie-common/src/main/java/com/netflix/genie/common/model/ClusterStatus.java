/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.wordnik.swagger.annotations.ApiModel;
import java.net.HttpURLConnection;
import org.apache.commons.lang3.StringUtils;

/**
 * The possible statuses for a cluster.
 *
 * @author tgianos
 */
@ApiModel(value = "Available statuses for a cluster")
public enum ClusterStatus {

    /**
     * Cluster is UP, and accepting jobs.
     */
    UP,
    /**
     * Cluster may be running, but not accepting job submissions.
     */
    OUT_OF_SERVICE,
    /**
     * Cluster is no-longer running, and is terminated.
     */
    TERMINATED;

    /**
     * Parse cluster status.
     *
     * @param value string to parse/convert into cluster status
     * @return UP, OUT_OF_SERVICE, TERMINATED if match
     * @throws CloudServiceException on invalid value
     */
    public static ClusterStatus parse(final String value) throws CloudServiceException {
        if (StringUtils.isBlank(value)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
                    "Unacceptable cluster status. Must be one of {UP, OUT_OF_SERVICE, TERMINATED}");
        } else if (value.equalsIgnoreCase("UP")) {
            return UP;
        } else if (value.equalsIgnoreCase("OUT_OF_SERVICE")) {
            return OUT_OF_SERVICE;
        } else if (value.equalsIgnoreCase("TERMINATED")) {
            return TERMINATED;
        } else {
            throw new CloudServiceException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
                    "Unacceptable cluster status. Must be one of {UP, OUT_OF_SERVICE, TERMINATED}");
        }
    }
}
