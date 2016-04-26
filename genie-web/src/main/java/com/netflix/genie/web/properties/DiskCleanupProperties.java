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
import org.springframework.stereotype.Component;

/**
 * Properties controlling the behavior of the database cleanup leadership task.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConfigurationProperties(prefix = "genie.tasks.diskCleanup")
@Component
@Getter
@Setter
public class DiskCleanupProperties {
    private boolean enabled;
    private String expression = "0 0 0 * * *";
    private int retention = 3;
}
