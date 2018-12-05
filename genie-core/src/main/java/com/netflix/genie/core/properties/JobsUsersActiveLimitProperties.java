/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.properties;

import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import java.util.Map;

/**
 * Properties related to user limits in number of active jobs.
 *
 * @author mprimi
 * @since 3.1.0
 */
@Getter
@Setter
@Validated
public class JobsUsersActiveLimitProperties {
    /**
     * Default value for active user job limit enabled.
     */
    public static final boolean DEFAULT_ENABLED = false;

    /**
     * Default value for active user job limit count.
     */
    public static final int DEFAULT_COUNT = 100;

    private boolean enabled = DEFAULT_ENABLED;
    @Min(value = 1)
    private int count = DEFAULT_COUNT;
    private Map<String, Integer> specialUser = Maps.newHashMap();

    public int getUserLimit(final String user) {
        return specialUser.getOrDefault(user, count);
    }
}
