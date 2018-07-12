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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Properties pertaining to how much memory jobs can use on Genie.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConfigurationProperties(prefix = JobsMemoryProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class JobsMemoryProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs.memory";

    /**
     * Default to 30 GB (30,720 MB).
     */
    private int maxSystemMemory = 30_720;

    /**
     * Default to 1.5 GB (1,536 MB).
     */
    private int defaultJobMemory = 1_024;

    /**
     * Defaults to 10 GB (10,240 MB).
     */
    private int maxJobMemory = 10_240;
}
