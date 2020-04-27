/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import com.netflix.genie.web.properties.ZookeeperProperties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.zookeeper.config.LeaderInitiatorFactoryBean;

/**
 * Unit tests for the {@link ZookeeperAutoConfiguration} class.
 *
 * @author mprimi
 * @since 4.0.0
 */
class ZookeeperAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    ZookeeperAutoConfiguration.class
                )
            );

    /**
     * Test expected beans when Zookeeper is not enabled.
     */
    @Test
    void expectedBeansWithZookeeperDisabled() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).doesNotHaveBean(ZookeeperProperties.class);
                Assertions.assertThat(context).doesNotHaveBean(LeaderInitiatorFactoryBean.class);
                Assertions.assertThat(context).doesNotHaveBean(ServiceDiscovery.class);
                Assertions.assertThat(context).doesNotHaveBean(Listenable.class);
            }
        );
    }

    /**
     * Test expected beans when Zookeeper is enabled.
     */
    @Test
    void expectedBeansWithZookeeperEnabled() {
        this.contextRunner
            .withUserConfiguration(ZookeeperMockConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ZookeeperProperties.class);
                    Assertions.assertThat(context).hasSingleBean(LeaderInitiatorFactoryBean.class);
                    Assertions.assertThat(context).hasSingleBean(ServiceDiscovery.class);
                    Assertions.assertThat(context).hasSingleBean(Listenable.class);
                }
            );
    }

    /**
     * Mock configuration for pretending zookeeper is enabled.
     */
    @Configuration
    static class ZookeeperMockConfig {

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance.
         */
        @Bean
        @SuppressWarnings("unchecked")
        CuratorFramework curatorFramework() {
            final CuratorFramework curatorFramework = Mockito.mock(CuratorFramework.class);
            Mockito
                .when(curatorFramework.getConnectionStateListenable())
                .thenReturn(Mockito.mock(Listenable.class));
            return curatorFramework;
        }
    }
}
