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
package com.netflix.genie.web.configs;

import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.properties.JobFileSyncRpcProperties;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcServerAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;

/**
 * Controls whether a gRPC server is configured for this Genie node or not.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@Import(GrpcServerAutoConfiguration.class)
@EnableConfigurationProperties(
    {
        GRpcServerProperties.class,
        JobFileSyncRpcProperties.class
    }
)
@Slf4j
public class GenieGRpcServerAutoConfiguration {

    /**
     * Log that gRPC server is enabled.
     */
    @PostConstruct
    public void postConstruct() {
        log.info("gRPC server configuration is ENABLED");
    }
}
