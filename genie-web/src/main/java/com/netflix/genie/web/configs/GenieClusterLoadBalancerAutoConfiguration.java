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
package com.netflix.genie.web.configs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.web.properties.ScriptLoadBalancerProperties;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.RandomizedClusterLoadBalancerImpl;
import com.netflix.genie.web.services.loadbalancers.script.ScriptLoadBalancer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;

/**
 * Auto configuration for providing a {@link ClusterLoadBalancer} for this Genie
 * instance.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        ScriptLoadBalancerProperties.class
    }
)
@Slf4j
public class GenieClusterLoadBalancerAutoConfiguration {

    /**
     * The relative order of the {@link ScriptLoadBalancer} if one is enabled relative to other
     * {@link ClusterLoadBalancer} instances that may be in the context. This allows users to fit {@literal 50} more
     * balancer's between the script load balancer and the default {@link RandomizedClusterLoadBalancerImpl}. If
     * the user wants to place a balancer implementation before the script one they only need to subtract from this
     * value.
     */
    public static final int SCRIPT_LOAD_BALANCER_PRECEDENCE = Ordered.LOWEST_PRECEDENCE - 50;

    /**
     * Produce the {@link ScriptLoadBalancer} instance to use for this Genie node if it was configured by the user.
     *
     * @param asyncTaskExecutor   The asynchronous task executor to use
     * @param taskScheduler       The task scheduler to use
     * @param fileTransferService The file transfer service to use
     * @param environment         The program environment from Spring
     * @param mapper              The JSON object mapper to use
     * @param registry            The meter registry for capturing metrics
     * @return A {@link ScriptLoadBalancer} if one enabled
     */
    @Bean
    @Order(SCRIPT_LOAD_BALANCER_PRECEDENCE)
    @ConditionalOnProperty(value = ScriptLoadBalancerProperties.ENABLED_PROPERTY, havingValue = "true")
    public ScriptLoadBalancer scriptLoadBalancer(
        @Qualifier("genieAsyncTaskExecutor") final AsyncTaskExecutor asyncTaskExecutor,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        @Qualifier("cacheGenieFileTransferService") final GenieFileTransferService fileTransferService,
        final Environment environment,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        log.info("Script load balancing is enabled. Creating a ScriptLoadBalancer.");
        return new ScriptLoadBalancer(
            asyncTaskExecutor,
            taskScheduler,
            fileTransferService,
            environment,
            mapper,
            registry
        );
    }

    /**
     * The default cluster load balancer if all others fail.
     * <p>
     * Defaults to {@link Ordered#LOWEST_PRECEDENCE}.
     *
     * @return A {@link RandomizedClusterLoadBalancerImpl} instance
     */
    @Bean
    @Order
    public RandomizedClusterLoadBalancerImpl randomizedClusterLoadBalancer() {
        return new RandomizedClusterLoadBalancerImpl();
    }
}
