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
package com.netflix.genie.common.internal.util;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Container for information about the host a Genie process (web server or agent) is running on.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString
public class GenieHostInfo {
    private final String hostname;

    /**
     * Constructor.
     *
     * @param hostname The hostname of the node this Genie process is running on.
     */
    public GenieHostInfo(final String hostname) {
        this.hostname = hostname;
    }
}
