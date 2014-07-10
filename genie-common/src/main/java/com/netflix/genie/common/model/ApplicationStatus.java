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
 * The available statuses for applications.
 *
 * @author tgianos
 */
@ApiModel(value = "Available statuses for an applicaiton")
public enum ApplicationStatus {

    /**
     * Application is active, and in-use.
     */
    ACTIVE,
    /**
     * Application is deprecated, and will be made inactive in the future.
     */
    DEPRECATED,
    /**
     * Application is inactive, and not in-use.
     */
    INACTIVE;

    /**
     * Parse config status.
     *
     * @param value string to parse/convert into config status
     * @return ACTIVE, DEPRECATED, INACTIVE if match
     * @throws CloudServiceException on invalid value
     */
    public static ApplicationStatus parse(final String value) throws CloudServiceException {
        if (StringUtils.isBlank(value)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
                    "Unacceptable application status. Must be one of {ACTIVE, DEPRECATED, INACTIVE}");
        } else if (value.equalsIgnoreCase("ACTIVE")) {
            return ACTIVE;
        } else if (value.equalsIgnoreCase("DEPRECATED")) {
            return DEPRECATED;
        } else if (value.equalsIgnoreCase("INACTIVE")) {
            return INACTIVE;
        } else {
            throw new CloudServiceException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
                    "Unacceptable application status. Must be one of {ACTIVE, DEPRECATED, INACTIVE}");
        }
    }
}
