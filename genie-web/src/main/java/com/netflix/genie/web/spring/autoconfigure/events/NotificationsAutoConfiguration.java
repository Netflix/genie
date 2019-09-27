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
package com.netflix.genie.web.spring.autoconfigure.events;

import com.amazonaws.services.sns.AmazonSNS;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.data.observers.PersistedJobStatusObserver;
import com.netflix.genie.web.data.observers.PersistedJobStatusObserverImpl;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedSNSPublisher;
import com.netflix.genie.web.events.JobNotificationMetricPublisher;
import com.netflix.genie.web.events.JobStateChangeSNSPublisher;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import com.netflix.genie.web.spring.autoconfigure.aws.AWSAutoConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Beans related to external notifications.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        SNSNotificationsProperties.class
    }
)
public class NotificationsAutoConfiguration {

    /**
     * Create {@link PersistedJobStatusObserver} if one does not exist.
     *
     * @param genieEventBus the genie event bus
     * @return a {@link PersistedJobStatusObserver}
     */
    @Bean
    @ConditionalOnMissingBean(PersistedJobStatusObserver.class)
    public PersistedJobStatusObserver persistedJobStatusObserver(
        final GenieEventBus genieEventBus
    ) {
        return new PersistedJobStatusObserverImpl(genieEventBus);
    }

    /**
     * Create a {@link JobNotificationMetricPublisher} which publishes metrics related to to job state changes
     * notifications.
     *
     * @param registry the metrics registry
     * @return a {@link JobNotificationMetricPublisher}
     */
    @Bean
    @ConditionalOnMissingBean(JobNotificationMetricPublisher.class)
    public JobNotificationMetricPublisher jobNotificationMetricPublisher(
        final MeterRegistry registry
    ) {
        return new JobNotificationMetricPublisher(registry);
    }

    /**
     * Create a {@link JobStateChangeSNSPublisher} unless one exists in the context already.
     *
     * @param snsClient  the Amazon SNS client
     * @param properties configuration properties
     * @param registry   the metrics registry
     * @return a {@link JobStateChangeSNSPublisher}
     */
    @Bean
    @ConditionalOnProperty(value = SNSNotificationsProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(JobStateChangeSNSPublisher.class)
    public JobStateChangeSNSPublisher jobNotificationsSNSPublisher(
        final SNSNotificationsProperties properties,
        final MeterRegistry registry,
        @Qualifier(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME) final AmazonSNS snsClient
    ) {
        return new JobStateChangeSNSPublisher(
            snsClient,
            properties,
            registry,
            GenieObjectMapper.getMapper()
        );
    }

    /**
     * Create a {@link JobFinishedSNSPublisher} unless one exists in the context already.
     *
     * @param properties            configuration properties
     * @param registry              the metrics registry
     * @param snsClient             the Amazon SNS client
     * @param jobPersistenceService the job persistence service
     * @return a {@link JobFinishedSNSPublisher}
     */
    @Bean
    @ConditionalOnProperty(value = SNSNotificationsProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(JobFinishedSNSPublisher.class)
    public JobFinishedSNSPublisher jobFinishedSNSPublisher(
        final SNSNotificationsProperties properties,
        final MeterRegistry registry,
        @Qualifier(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME) final AmazonSNS snsClient,
        final JobPersistenceService jobPersistenceService
    ) {
        return new JobFinishedSNSPublisher(
            snsClient,
            properties,
            jobPersistenceService,
            registry,
            GenieObjectMapper.getMapper()
        );
    }
}
