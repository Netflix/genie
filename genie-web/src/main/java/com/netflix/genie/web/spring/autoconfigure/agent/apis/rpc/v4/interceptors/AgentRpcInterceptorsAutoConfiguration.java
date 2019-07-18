/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.v4.interceptors;

import com.netflix.genie.web.agent.apis.rpc.v4.interceptors.SimpleLoggingInterceptor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Spring Auto Configuration for default {@link io.grpc.ServerInterceptor} implementations for the Genie gRPC services.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class AgentRpcInterceptorsAutoConfiguration {

    /**
     * An interceptor which writes requests to the logs.
     *
     * @return Instance of {@link SimpleLoggingInterceptor}
     */
    @Bean
    @ConditionalOnMissingBean(SimpleLoggingInterceptor.class)
    @Order // Defaults to lowest precedence when stored in a list
    public SimpleLoggingInterceptor simpleLoggingInterceptor() {
        return new SimpleLoggingInterceptor();
    }
}
