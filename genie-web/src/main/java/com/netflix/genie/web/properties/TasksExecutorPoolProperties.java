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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * Properties related to the thread pool for the task executor within Genie.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = TasksExecutorPoolProperties.PROPERTY_PREFIX)
@Validated
@Getter
@Setter
public class TasksExecutorPoolProperties {

    /**
     * The property prefix for this group.
     */
    public static final String PROPERTY_PREFIX = "genie.tasks.executor.pool";

    /**
     * The number of threads desired for this system. Likely best to do one more than number of CPUs.
     */
    @Min(1)
    private int size = 2;

    /**
     * The name prefix to apply to threads from this pool.
     */
    @NotBlank(message = "A thread prefix name is required")
    private String threadNamePrefix = "genie-task-executor-";
}
