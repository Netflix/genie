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
package com.netflix.genie.web.spring.autoconfigure.dtos;

import com.amazonaws.util.EC2MetadataUtils;
import com.netflix.genie.web.agent.apis.rpc.servers.GRpcServerUtils;
import com.netflix.genie.web.dtos.GenieWebHostInfo;
import com.netflix.genie.web.dtos.GenieWebRpcInfo;
import com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.servers.AgentRpcServersAutoConfiguration;
import io.grpc.Server;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.aws.context.support.env.AwsCloudEnvironmentCheckUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Auto configuration for shared DTO instances within the web server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@AutoConfigureAfter(
    {
        AgentRpcServersAutoConfiguration.class
    }
)
public class DtosAutoConfiguration {

    /**
     * Get the {@link GenieWebHostInfo} for this application. This is the default fallback implementation if no other
     * bean instance of this type has been created.
     *
     * @return A {@link GenieWebHostInfo} instance
     * @throws UnknownHostException  When the host can't be calculated
     * @throws IllegalStateException When an instance can't be created
     * @see InetAddress#getCanonicalHostName()
     */
    @Bean
    @ConditionalOnMissingBean(GenieWebHostInfo.class)
    public GenieWebHostInfo genieHostInfo() throws UnknownHostException {
        final String hostname;
        if (AwsCloudEnvironmentCheckUtils.isRunningOnCloudEnvironment()) {
            hostname = EC2MetadataUtils.getPrivateIpAddress();
        } else {
            // Fallback if not on AWS
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        }

        if (StringUtils.isBlank(hostname)) {
            throw new IllegalStateException("Unable to create a Genie Host Info instance as hostname is blank");
        }

        return new GenieWebHostInfo(hostname);
    }

    /**
     * Provide a {@link GenieWebRpcInfo} bean if one hasn't already been defined.
     *
     * @param server The gRPC {@link Server} instance. Must not be {@link Server#isShutdown()} or
     *               {@link Server#isTerminated()}. Must be able to get the port the server is listening on.
     * @return A {@link GenieWebRpcInfo} instance
     * @throws IllegalStateException When an instance can't be created
     */
    @Bean
    @ConditionalOnMissingBean(
        {
            GenieWebRpcInfo.class
        }
    )
    public GenieWebRpcInfo genieWebRpcInfo(final Server server) throws IllegalStateException {
        if (server.isShutdown() || server.isTerminated()) {
            throw new IllegalStateException("gRPC server is already shut down. Can't start.");
        } else {
            final int port = GRpcServerUtils.startServer(server);
            if (port < 1) {
                throw new IllegalStateException("gRPC server started on illegal port: " + port);
            }
            return new GenieWebRpcInfo(port);
        }
    }
}
