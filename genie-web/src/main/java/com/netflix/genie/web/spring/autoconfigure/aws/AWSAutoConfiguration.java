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
package com.netflix.genie.web.spring.autoconfigure.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.netflix.genie.web.properties.RetryProperties;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * AWS beans.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        RetryProperties.class,
        SNSNotificationsProperties.class,
    }
)
public class AWSAutoConfiguration {

    /**
     * The name of the {@link AmazonSNS} client created specifically for job state notifications.
     *
     * Note: this name must match the bean name defined in
     * {@link org.springframework.cloud.aws.messaging.config.annotation.SNSConfiguration} in order to override it.
     */
    public static final String SNS_CLIENT_BEAN_NAME = "amazonSNS";
    private static final String SNS_CLIENT_CONFIGURATION_BEAN_NAME = "SNSClientConfiguration";
    private static final String SNS_CLIENT_RETRY_POLICY_BEAN_NAME = "SNSClientRetryPolicy";

    /**
     * Create a named {@link RetryPolicy} to be used by the {@link AmazonSNS} client, unless a bean by that name
     * already exists in context.
     *
     * @param retryProperties The retry properties
     * @return a named {@link RetryPolicy}
     */
    @Bean(name = SNS_CLIENT_RETRY_POLICY_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_RETRY_POLICY_BEAN_NAME)
    public RetryPolicy jobNotificationsSNSClientRetryPolicy(
        final RetryProperties retryProperties
    ) {
        return PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(
            retryProperties.getSns().getNoOfRetries()
        );
    }

    /**
     * Create a named {@link ClientConfiguration} to be used by the {@link AmazonSNS} client, unless a bean by that
     * name already exists in context.
     *
     * @param retryPolicy The retry policy
     * @return a named {@link ClientConfiguration}
     */
    @Bean(name = SNS_CLIENT_CONFIGURATION_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_CONFIGURATION_BEAN_NAME)
    public ClientConfiguration jobNotificationsSNSClientConfiguration(
        @Qualifier(SNS_CLIENT_RETRY_POLICY_BEAN_NAME) final RetryPolicy retryPolicy
    ) {
        final ClientConfiguration configuration = new ClientConfigurationFactory().getConfig();
        configuration.setRetryPolicy(retryPolicy);
        return configuration;
    }

    /**
     * Create a named {@link AmazonSNS} client to be used by JobNotification SNS publishers, unless a bean by that
     * name already exists in context.
     *
     * @param credentialsProvider The credentials provider
     * @param awsRegionProvider   The region provider
     * @param clientConfiguration The client configuration
     * @return an {@link AmazonSNS} client
     */
    @Bean(name = SNS_CLIENT_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_BEAN_NAME)
    @ConditionalOnProperty(value = SNSNotificationsProperties.ENABLED_PROPERTY, havingValue = "true")
    public AmazonSNS jobNotificationsSNSClient(
        final AWSCredentialsProvider credentialsProvider,
        final AwsRegionProvider awsRegionProvider,
        @Qualifier(SNS_CLIENT_CONFIGURATION_BEAN_NAME) final ClientConfiguration clientConfiguration
    ) {
        return AmazonSNSClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(awsRegionProvider.getRegion())
            .withClientConfiguration(clientConfiguration)
            .build();
    }
}
