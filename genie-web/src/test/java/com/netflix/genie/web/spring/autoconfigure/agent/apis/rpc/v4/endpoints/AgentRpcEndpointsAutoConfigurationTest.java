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
package com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.v4.endpoints;

import com.netflix.genie.common.internal.configs.ProtoConvertersAutoConfiguration;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcAgentFileStreamServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcHeartBeatServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcJobServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcPingServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.JobServiceProtoErrorComposer;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.JobSearchService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;

/**
 * Tests for {@link AgentRpcEndpointsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class AgentRpcEndpointsAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    AgentRpcEndpointsAutoConfiguration.class,
                    ProtoConvertersAutoConfiguration.class
                )
            )
            .withUserConfiguration(RequiredBeans.class);

    /**
     * Default beans created.
     */
    @Test
    void expectedBeansExistIfGrpcEnabledAndNoUserBeans() {
        this.contextRunner
            .run(
                context -> {
                    Assertions.assertThat(context.containsBean("heartBeatServiceTaskScheduler")).isTrue();
                    Assertions.assertThat(context).hasSingleBean(JobServiceProtoErrorComposer.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(FileStreamServiceGrpc.FileStreamServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(GRpcAgentFileStreamServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(HeartBeatServiceGrpc.HeartBeatServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(GRpcHeartBeatServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(JobKillServiceGrpc.JobKillServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(GRpcJobServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(JobServiceGrpc.JobServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(GRpcJobServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(PingServiceGrpc.PingServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(GRpcPingServiceImpl.class);
                }
            );
    }

    /**
     * User beans override defaults.
     */
    @Test
    void expectedBeansExistWhenUserOverrides() {
        this.contextRunner
            .withUserConfiguration(UserConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context.containsBean("heartBeatServiceTaskScheduler")).isTrue();
                    Assertions.assertThat(context).hasSingleBean(JobServiceProtoErrorComposer.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(FileStreamServiceGrpc.FileStreamServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .doesNotHaveBean(GRpcAgentFileStreamServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(HeartBeatServiceGrpc.HeartBeatServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .doesNotHaveBean(GRpcHeartBeatServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(JobKillServiceGrpc.JobKillServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .doesNotHaveBean(GRpcJobServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(JobServiceGrpc.JobServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .doesNotHaveBean(GRpcJobServiceImpl.class);
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(PingServiceGrpc.PingServiceImplBase.class);
                    Assertions
                        .assertThat(context)
                        .doesNotHaveBean(GRpcPingServiceImpl.class);

                    Assertions.assertThat(context.containsBean("userJobServiceProtoErrorComposer")).isTrue();
                }
            );
    }

    /**
     * Mocking needed required beans provided by other configurations.
     */
    @Configuration
    static class RequiredBeans {
        @Bean
        TaskScheduler genieTaskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean
        AgentRoutingService agentRoutingService() {
            return Mockito.mock(AgentRoutingService.class);
        }

        @Bean
        JobSearchService jobSearchService() {
            return Mockito.mock(JobSearchService.class);
        }

        @Bean
        AgentJobService agentJobService() {
            return Mockito.mock(AgentJobService.class);
        }

        @Bean
        GenieHostInfo genieHostInfo() {
            return Mockito.mock(GenieHostInfo.class);
        }
    }

    /**
     * Dummy user configuration.
     */
    @Configuration
    static class UserConfig {

        @Bean(name = "heartBeatServiceTaskScheduler")
        TaskScheduler userHeartBeatScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean
        JobServiceProtoErrorComposer userJobServiceProtoErrorComposer() {
            return Mockito.mock(JobServiceProtoErrorComposer.class);
        }

        @Bean
        FileStreamServiceGrpc.FileStreamServiceImplBase userGRpcAgentFileStreamService() {
            return Mockito.mock(FileStreamServiceGrpc.FileStreamServiceImplBase.class);
        }

        @Bean
        HeartBeatServiceGrpc.HeartBeatServiceImplBase userGRpcHeartBeatService() {
            return Mockito.mock(HeartBeatServiceGrpc.HeartBeatServiceImplBase.class);
        }

        @Bean
        JobKillServiceGrpc.JobKillServiceImplBase userGRpcJobKillService() {
            return Mockito.mock(JobKillServiceGrpc.JobKillServiceImplBase.class);
        }

        @Bean
        JobServiceGrpc.JobServiceImplBase userGRpcJobService() {
            return Mockito.mock(JobServiceGrpc.JobServiceImplBase.class);
        }

        @Bean
        PingServiceGrpc.PingServiceImplBase userGRpcPingService() {
            return Mockito.mock(PingServiceGrpc.PingServiceImplBase.class);
        }
    }
}
