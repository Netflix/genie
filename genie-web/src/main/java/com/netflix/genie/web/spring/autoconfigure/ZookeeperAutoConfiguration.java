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

import com.netflix.genie.web.agent.services.impl.AgentRoutingServiceCuratorDiscoveryImpl;
import com.netflix.genie.web.properties.ZookeeperProperties;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.zookeeper.config.LeaderInitiatorFactoryBean;

/**
 * Auto configuration for Zookeper components.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        ZookeeperProperties.class
    }
)
@AutoConfigureAfter(
    {
        org.springframework.cloud.zookeeper.ZookeeperAutoConfiguration.class
    }
)
@ConditionalOnBean(CuratorFramework.class)
public class ZookeeperAutoConfiguration {

    /**
     * The leadership initialization factory bean which will create a LeaderInitiator to kick off the leader election
     * process within this node for the cluster if Zookeeper is configured.
     *
     * @param client              The curator framework client to use
     * @param zookeeperProperties The Zookeeper properties to use
     * @return The factory bean
     */
    @Bean
    @ConditionalOnMissingBean(LeaderInitiatorFactoryBean.class)
    public LeaderInitiatorFactoryBean leaderInitiatorFactory(
        final CuratorFramework client,
        final ZookeeperProperties zookeeperProperties
    ) {
        final LeaderInitiatorFactoryBean factoryBean = new LeaderInitiatorFactoryBean();
        factoryBean.setClient(client);
        factoryBean.setPath(zookeeperProperties.getLeaderPath());
        factoryBean.setRole("cluster");
        return factoryBean;
    }

    /**
     * The Curator-based Service Discovery bean.
     *
     * @param client              The curator framework client to use
     * @param zookeeperProperties The Zookeeper properties to use
     * @return {@link ServiceDiscovery} bean for instances of type {@link AgentRoutingServiceCuratorDiscoveryImpl.Agent}
     */
    @Bean
    @ConditionalOnMissingBean(ServiceDiscovery.class)
    ServiceDiscovery<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceDiscovery(
        final CuratorFramework client,
        final ZookeeperProperties zookeeperProperties
    ) {
        return ServiceDiscoveryBuilder.builder(AgentRoutingServiceCuratorDiscoveryImpl.Agent.class)
            .basePath(zookeeperProperties.getDiscoveryPath())
            .client(client)
            .build();
    }

    /**
     * The Curator-client connection state listenable.
     *
     * @param client The curator framework client to use
     * @return {@link ServiceDiscovery} bean for instances of type {@link AgentRoutingServiceCuratorDiscoveryImpl.Agent}
     */
    @Bean
    @ConditionalOnMissingBean(Listenable.class)
    Listenable<ConnectionStateListener> listenableCuratorConnectionState(
        final CuratorFramework client
    ) {
        return client.getConnectionStateListenable();
    }

}
