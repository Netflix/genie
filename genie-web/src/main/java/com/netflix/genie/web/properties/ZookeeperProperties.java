/*
 *
 *  Copyright 2017 Netflix, Inc.
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
 * Properties related to Zookeeper.
 *
 * @author tgianos
 * @since 3.1.0
 */
@ConfigurationProperties(prefix = ZookeeperProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class ZookeeperProperties {

    /**
     * The property prefix for this group.
     */
    public static final String PROPERTY_PREFIX = "genie.zookeeper";

    /**
     * The base Zookeeper node path for Genie leadership.
     */
    private String leaderPath = "/genie/leader/";

    /**
     * The base Zookeeper node path for discovery.
     */
    private String discoveryPath = "/genie/agents/";
}
