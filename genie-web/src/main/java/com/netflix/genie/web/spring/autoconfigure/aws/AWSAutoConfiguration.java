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

import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import org.springframework.context.annotation.Bean;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.services.sns.SnsClient;
import com.netflix.genie.web.properties.RetryProperties;
import com.netflix.genie.web.properties.SNSNotificationsProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

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
     */
    public static final String SNS_CLIENT_BEAN_NAME = "snsClient";
    public static final String SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME = "snsClientOverrideConfig";

    /**
     * Create a named {@link ClientOverrideConfiguration} to be used by the {@link SnsClient}, unless a bean by that name
     * already exists in context.
     *
     * @param retryProperties The retry properties
     * @return a named {@link ClientOverrideConfiguration}
     */
    @Bean(name = SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME)
    public ClientOverrideConfiguration jobNotificationsSNSClientOverrideConfig(
        final RetryProperties retryProperties
    ) {
        return ClientOverrideConfiguration.builder()
            .retryStrategy(RetryMode.STANDARD)
            .apiCallTimeout(Duration.ofSeconds(retryProperties.getSns().getApiCallTimeoutSeconds()))
            .apiCallAttemptTimeout(Duration.ofSeconds(retryProperties.getSns().getApiCallAttemptTimeoutSeconds()))
            .build();
    }

    /**
     * Create a named {@link SnsClient} to be used by JobNotification SNS publishers, unless a bean by that
     * name already exists in context.
     *
     * @param credentialsProvider The credentials provider
     * @param awsRegionProvider   The region provider
     * @param overrideConfig      The client override configuration
     * @return an {@link SnsClient}
     */
    @Bean(name = SNS_CLIENT_BEAN_NAME)
    @ConditionalOnMissingBean(name = SNS_CLIENT_BEAN_NAME)
    @ConditionalOnProperty(value = SNSNotificationsProperties.ENABLED_PROPERTY, havingValue = "true")
    public SnsClient jobNotificationsSNSClient(
        final AwsCredentialsProvider credentialsProvider,
        final AwsRegionProvider awsRegionProvider,
        @Qualifier(SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME) final ClientOverrideConfiguration overrideConfig
    ) {
        return SnsClient.builder()
            .credentialsProvider(credentialsProvider)
            .region(awsRegionProvider.getRegion())
            .overrideConfiguration(overrideConfig)
            .build();
    }
}
