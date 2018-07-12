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
import javax.validation.constraints.NotEmpty;

/**
 * Properties related to the {@link com.netflix.genie.web.services.loadbalancers.script.ScriptLoadBalancer}
 * implementation.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = ScriptLoadBalancerProperties.PROPERTY_PREFIX)
@Validated
@Getter
@Setter
public class ScriptLoadBalancerProperties {
    /**
     * The common prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs.clusters.load-balancers.script";

    /**
     * Feature flag constant. Property with this key should be true if this feature should be enabled.
     */
    public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

    /**
     * How often the script should be refreshed from the source.
     */
    public static final String REFRESH_RATE_PROPERTY = PROPERTY_PREFIX + ".refreshRate";

    /**
     * Where the script should be stored on local disk.
     */
    public static final String SCRIPT_FILE_DESTINATION_PROPERTY = PROPERTY_PREFIX + ".destination";

    /**
     * The source location of the script.
     */
    public static final String SCRIPT_FILE_SOURCE_PROPERTY = PROPERTY_PREFIX + ".source";

    /**
     * The timeout for script evaluation.
     */
    public static final String TIMEOUT_PROPERTY = PROPERTY_PREFIX + ".timeout";

    private boolean enabled;
    @Min(1L)
    private long refreshRate = 300_000L;
    @NotEmpty
    private String destination = "file:///tmp/genie/loadbalancers/script/destination/";
    @NotEmpty
    private String source = "file:///tmp/genie/loadBalancers/script/source/loadBalance.js";
    @Min(1L)
    private long timeout = 5_000L;
}
