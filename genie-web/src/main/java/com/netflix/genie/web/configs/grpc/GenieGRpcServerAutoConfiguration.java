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
package com.netflix.genie.web.configs.grpc;

import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.rpc.grpc.servers.GRpcServerManager;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Set;

/**
 * Controls whether a gRPC server is configured and started for this Genie node or not.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@EnableConfigurationProperties(
    {
        GRpcServerProperties.class,
    }
)
@AutoConfigureAfter(
    name = {
        "com.netflix.springboot.grpc.server.GrpcServerAutoConfiguration"
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

    /**
     * Create a {@link Server} if one isn't already present in the context.
     *
     * @param port               The port this server should listen on
     * @param services           The gRPC services this server should serve
     * @param serverInterceptors The {@link ServerInterceptor} implementations that should be applied to all services
     * @return A Netty server instance based on the provided information
     */
    @Bean
    @ConditionalOnMissingBean(Server.class)
    public Server gRpcServer(
        @Value("${grpc.server.port:0}") final int port,  // TODO: finalize how to get configure this property
        final Set<BindableService> services,
        final List<ServerInterceptor> serverInterceptors
    ) {
        final NettyServerBuilder builder = NettyServerBuilder.forPort(port);

        // Add Service interceptors and add services to the server
        services
            .stream()
            .map(BindableService::bindService)
            .map(serviceDefinition -> ServerInterceptors.intercept(serviceDefinition, serverInterceptors))
            .forEach(builder::addService);

        return builder.build();
    }

    /**
     * Create a {@link GRpcServerManager} instance to manage the lifecycle of the gRPC server if one isn't already
     * defined.
     *
     * @param server The {@link Server} instance to manage
     * @return A {@link GRpcServerManager} instance
     */
    @Bean
    @ConditionalOnMissingBean(GRpcServerManager.class)
    public GRpcServerManager gRpcServerManager(final Server server) {
        return new GRpcServerManager(server);
    }
}
