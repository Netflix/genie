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

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.netflix.genie.web.properties.RetryProperties;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;

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
     * The name of the {@link SnsClient} client created specifically for job state notifications.
     * <p>
     * Note: this name must match the bean name defined in
     * {@link io.awspring.cloud.messaging.config.annotation.SnsConfiguration} in order to override it.
     */
    public static final String SNS_CLIENT_BEAN_NAME = "amazonSNS";
    private static final String SNS_CLIENT_CONFIGURATION_BEAN_NAME = "SNSClientConfiguration";
    private static final String SNS_CLIENT_RETRY_POLICY_BEAN_NAME = "SNSClientRetryPolicy";

    /**
     * Create a named {@link RetryPolicy} to be used by the {@link SnsClient} client, unless a bean by that name
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
     * Create a named {@link ClientOverrideConfiguration} to be used by the {@link SnsClient} client, unless a bean by that
     * name already exists in context.
     *
     * @param retryPolicy The retry policy
     * @return a named {@link ClientOverrideConfiguration}
     */
    @Bean(name = SNS_CLIENT_CONFIGURATION_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_CONFIGURATION_BEAN_NAME)
    public ClientOverrideConfiguration jobNotificationsSNSClientConfiguration(
        @Qualifier(SNS_CLIENT_RETRY_POLICY_BEAN_NAME) final RetryPolicy retryPolicy
    ) {
        final ClientOverrideConfiguration configuration = new ClientConfigurationFactory().getConfig();
        configuration.retryPolicy(retryPolicy);
        return configuration;
    }

    /**
     * Create a named {@link SnsClient} client to be used by JobNotification SNS publishers, unless a bean by that
     * name already exists in context.
     *
     * @param credentialsProvider The credentials provider
     * @param awsRegionProvider   The region provider
     * @param clientConfiguration The client configuration
     * @return an {@link SnsClient} client
     */
    @Bean(name = SNS_CLIENT_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_BEAN_NAME)
    @ConditionalOnProperty(value = SNSNotificationsProperties.ENABLED_PROPERTY, havingValue = "true")
    public SnsClient jobNotificationsSNSClient(
        final AwsCredentialsProvider credentialsProvider,
        final AwsRegionProvider awsRegionProvider,
        @Qualifier(SNS_CLIENT_CONFIGURATION_BEAN_NAME) final ClientOverrideConfiguration clientConfiguration
    ) {
        return SnsClient.builder()
            .credentialsProvider(credentialsProvider)
            .region(Region.of(awsRegionProvider.getRegion()))
            .overrideConfiguration(clientConfiguration)
            .build();
    }
}
