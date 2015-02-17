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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.wordnik.swagger.annotations.ApiModel;
import org.apache.commons.lang3.StringUtils;


/**
 * The available statuses for Commands.
 *
 * @author tgianos
 */
@ApiModel(value = "Available statuses for a command")
public enum CommandStatus {

    /**
     * Command is active, and in-use.
     */
    ACTIVE,
    /**
     * Command is deprecated, and will be made inactive in the future.
     */
    DEPRECATED,
    /**
     * Command is inactive, and not in-use.
     */
    INACTIVE;

    /**
     * Parse command status.
     *
     * @param value string to parse/convert into command status
     * @return ACTIVE, DEPRECATED, INACTIVE if match
     * @throws GeniePreconditionException on invalid value
     */
    public static CommandStatus parse(final String value) throws GeniePreconditionException {
        if (StringUtils.isNotBlank(value)) {
            for (final CommandStatus status : CommandStatus.values()) {
                if (value.equalsIgnoreCase(status.toString())) {
                    return status;
                }
            }
        }
        throw new GeniePreconditionException(
                "Unacceptable command status. Must be one of {ACTIVE, DEPRECATED, INACTIVE}");
    }
}
