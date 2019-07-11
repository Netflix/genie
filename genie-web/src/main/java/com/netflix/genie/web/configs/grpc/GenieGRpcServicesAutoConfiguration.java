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
package com.netflix.genie.web.configs.grpc;

import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.rpc.grpc.services.impl.v4.GRpcAgentFileStreamServiceImpl;
import com.netflix.genie.web.rpc.grpc.services.impl.v4.GRpcHeartBeatServiceImpl;
import com.netflix.genie.web.rpc.grpc.services.impl.v4.GRpcJobKillServiceImpl;
import com.netflix.genie.web.rpc.grpc.services.impl.v4.GRpcJobServiceImpl;
import com.netflix.genie.web.rpc.grpc.services.impl.v4.GRpcPingServiceImpl;
import com.netflix.genie.web.rpc.grpc.services.impl.v4.JobServiceProtoErrorComposer;
import com.netflix.genie.web.services.AgentJobService;
import com.netflix.genie.web.services.AgentRoutingService;
import com.netflix.genie.web.services.JobSearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Configures various gRPC services and related beans if gRPC functionality is enabled.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@Slf4j
public class GenieGRpcServicesAutoConfiguration {

    private static final int SINGLE_THREAD = 1;

    /**
     * Get the task scheduler used by the HeartBeat Service.
     *
     * @return The task scheduler
     */
    @Bean
    @ConditionalOnMissingBean(name = "heartBeatServiceTaskScheduler")
    public TaskScheduler heartBeatServiceTaskScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(SINGLE_THREAD);
        return scheduler;
    }

    /**
     * Bean for converting errors in the job service to gRPC messages.
     *
     * @return An instance of {@link JobServiceProtoErrorComposer}
     */
    @Bean
    @ConditionalOnMissingBean(JobServiceProtoErrorComposer.class)
    public JobServiceProtoErrorComposer jobServiceProtoErrorComposer() {
        return new JobServiceProtoErrorComposer();
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.FileStreamServiceGrpc.FileStreamServiceImplBase}
     * if no other is provided.
     *
     * @param converter     The {@link JobDirectoryManifestProtoConverter} instance to use
     * @param taskScheduler The {@link TaskScheduler} to use to schedule tasks
     * @return An instance of {@link GRpcAgentFileStreamServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(FileStreamServiceGrpc.FileStreamServiceImplBase.class)
    public GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService(
        final JobDirectoryManifestProtoConverter converter,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler
    ) {
        return new GRpcAgentFileStreamServiceImpl(converter, taskScheduler);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.HeartBeatServiceGrpc.HeartBeatServiceImplBase}
     * if no other is provided.
     *
     * @param agentRoutingService The {@link AgentRoutingService} implementation to use
     * @param taskScheduler       The {@link TaskScheduler} instance to use
     * @return A {@link GRpcHeartBeatServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(HeartBeatServiceGrpc.HeartBeatServiceImplBase.class)
    public GRpcHeartBeatServiceImpl gRpcHeartBeatService(
        final AgentRoutingService agentRoutingService,
        @Qualifier("heartBeatServiceTaskScheduler") final TaskScheduler taskScheduler
    ) {
        return new GRpcHeartBeatServiceImpl(agentRoutingService, taskScheduler);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.JobKillServiceGrpc.JobKillServiceImplBase}
     * if no other is provided.
     *
     * @param jobSearchService The {@link JobSearchService} instance to use
     * @return A {@link GRpcJobKillServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobKillServiceGrpc.JobKillServiceImplBase.class)
    public GRpcJobKillServiceImpl gRpcJobKillService(final JobSearchService jobSearchService) {
        return new GRpcJobKillServiceImpl(jobSearchService);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceImplBase} if no other is
     * provided.
     *
     * @param agentJobService          The {@link AgentJobService} instance to use
     * @param jobServiceProtoConverter The {@link JobServiceProtoConverter} instance to use
     * @param protoErrorComposer       The {@link JobServiceProtoErrorComposer} instance to use
     * @return A {@link GRpcJobServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobServiceGrpc.JobServiceImplBase.class)
    public GRpcJobServiceImpl gRpcJobService(
        final AgentJobService agentJobService,
        final JobServiceProtoConverter jobServiceProtoConverter,
        final JobServiceProtoErrorComposer protoErrorComposer
    ) {
        return new GRpcJobServiceImpl(agentJobService, jobServiceProtoConverter, protoErrorComposer);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.PingServiceGrpc.PingServiceImplBase} if no
     * other is provided.
     *
     * @param genieHostInfo The information about the Genie host
     * @return Instance of {@link GRpcPingServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(PingServiceGrpc.PingServiceImplBase.class)
    public GRpcPingServiceImpl gRpcPingService(final GenieHostInfo genieHostInfo) {
        return new GRpcPingServiceImpl(genieHostInfo);
    }
}
