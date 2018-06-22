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
package com.netflix.genie.security.configs;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

import javax.annotation.PostConstruct;

/**
 * Controls whether a Redis connection is configured for this Genie node or not.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Configuration
@ConditionalOnProperty(value = "genie.redis.enabled", havingValue = "true")
@Import(RedisAutoConfiguration.class)
@Slf4j
public class RedisConfig {

    /**
     * Log that Redis is enabled.
     */
    @PostConstruct
    public void postConstruct() {
        log.info("Redis configuration is ENABLED");
    }

    /**
     * Whether or not we should enable Redis data repositories.
     *
     * @author tgianos
     * @since 3.1.0
     */
    @Configuration
    @ConditionalOnProperty(
        name = {
            "genie.redis.enabled",
            "spring.data.redis.repositories.enabled"
        },
        havingValue = "true"
    )
    @EnableRedisRepositories("com.netflix.genie")
    public static class EnableRedisRepositoryConfig {
    }
}
