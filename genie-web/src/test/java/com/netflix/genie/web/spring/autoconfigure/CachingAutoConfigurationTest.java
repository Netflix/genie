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
package com.netflix.genie.web.spring.autoconfigure;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;

/**
 * Tests for {@link CachingAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class CachingAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    CacheAutoConfiguration.class,
                    CachingAutoConfiguration.class
                )
            );

    /**
     * The auto configuration creates the expected beans.
     */
    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                // This should be provided by the Spring Boot starter after @EnableCaching is applied
                Assertions.assertThat(context).hasSingleBean(CacheManager.class);
                Assertions.assertThat(context).hasSingleBean(CaffeineCacheManager.class);
            }
        );
    }
}
