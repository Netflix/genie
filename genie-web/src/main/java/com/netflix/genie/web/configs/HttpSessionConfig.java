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
package com.netflix.genie.web.configs;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;

/**
 * Class to replace HTTP Session from Tomcat with one persisted to Redis for sharing session across a cluster.
 *
 * @author tgianos
 * @since 3.0.0
 */
//@ConditionalOnProperty("genie.redis.enabled")
//@Import(RedisAutoConfiguration.class)
//@EnableRedisHttpSession
@Slf4j
public class HttpSessionConfig {

    /**
     * Log out that Redis based HTTP Session storage is enabled.
     */
    @PostConstruct
    public void postConstruct() {
        log.info("Spring Session with Redis as storage is ENABLED");
    }
}
