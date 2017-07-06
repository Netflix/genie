/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie;

import com.google.common.collect.Maps;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.Map;

/**
 * Main Genie Spring Configuration class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@SpringBootApplication(
    exclude = {
        RedisAutoConfiguration.class,
        RedisRepositoriesAutoConfiguration.class
    }
)
@EnableAspectJAutoProxy
public class GenieWeb {

    static final String SPRING_CONFIG_LOCATION = "spring.config.location";
    static final String USER_HOME_GENIE = "${user.home}/.genie/";

    /**
     * Protected constructor.
     */
    protected GenieWeb() {
    }

    /**
     * Spring Boot Main.
     *
     * @param args Program arguments
     * @throws Exception For any failure during program execution
     */
    public static void main(final String[] args) throws Exception {
        final SpringApplication genie = new SpringApplication(GenieWeb.class);
        genie.setDefaultProperties(getDefaultProperties());
        genie.run(args);
    }

    static Map<String, Object> getDefaultProperties() {
        final Map<String, Object> defaultProperties = Maps.newHashMap();
        defaultProperties.put(SPRING_CONFIG_LOCATION, USER_HOME_GENIE);
        return defaultProperties;
    }
}
