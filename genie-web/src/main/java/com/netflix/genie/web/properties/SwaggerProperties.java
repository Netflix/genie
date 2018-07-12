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

/**
 * Properties related to Genie and swagger.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = SwaggerProperties.PROPERTY_PREFIX)
@Validated
@Getter
@Setter
public class SwaggerProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.swagger";

    /**
     * The property that reflects whether this feature is enabled or not.
     */
    public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

    private boolean enabled;
}
