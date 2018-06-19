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
package com.netflix.genie;

import net.devh.springboot.autoconfigure.grpc.server.GrpcServerAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * A class that serves to set up Spring Boot within a servlet container rather than an embedded one.
 *
 * @author tgianos
 * @since 3.0.0
 */
@SpringBootApplication(
    exclude = {
        RedisAutoConfiguration.class,
        GrpcServerAutoConfiguration.class,
    }
)
public class GenieWar extends SpringBootServletInitializer {

    /**
     * Spring Boot Main.
     *
     * @param args Program arguments
     * @throws Exception For any failure during program execution
     */
    public static void main(final String[] args) throws Exception {
        SpringApplication.run(GenieWar.class, args);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SpringApplicationBuilder configure(final SpringApplicationBuilder application) {
        return application.sources(GenieWar.class);
    }
}
