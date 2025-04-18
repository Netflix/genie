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

import io.awspring.cloud.autoconfigure.sns.SnsAutoConfiguration;
import io.awspring.cloud.autoconfigure.core.CredentialsProviderAutoConfiguration;
import io.awspring.cloud.autoconfigure.core.RegionProviderAutoConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.sns.SnsClient;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for behavior of {@link AWSAutoConfiguration}.
 *
 * @author mprimi
 * @since 4.0.0
 */
class AWSAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                AWSAutoConfiguration.class,
                CredentialsProviderAutoConfiguration.class,
                RegionProviderAutoConfiguration.class,
                SnsAutoConfiguration.class,
                com.netflix.genie.common.internal.configs.AwsAutoConfiguration.class
            )
        )
        .withPropertyValues(
            "genie.retry.sns.api-call-timeout-seconds=10",
            "genie.retry.sns.api-call-attempt-timeout-seconds=5",
            "genie.notifications.sns.enabled=true",
            "spring.cloud.aws.credentials.use-default-aws-credentials-chain=true",
            "spring.cloud.aws.region.auto=false",
            "spring.cloud.aws.region.static=us-east-1",
            "spring.cloud.aws.stack.auto=false",
            "spring.jmx.enabled=false",
            "spring.main.webApplicationType=none"
        );

    /**
     * Test expected context.
     */
    @Test
    void testExpectedContext() {
        this.contextRunner.run(
            (context) -> {
                // Verify beans exist
                Assertions.assertThat(context).hasBean(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME);
                Assertions.assertThat(context).hasBean(AWSAutoConfiguration.SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME);

                // Verify configuration
                ClientOverrideConfiguration overrideConfig = context.getBean(
                    AWSAutoConfiguration.SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME,
                    ClientOverrideConfiguration.class
                );

                // Verify retry strategy is set (we can't directly check the mode)
                Assertions.assertThat(overrideConfig.retryStrategy()).isPresent();

                // Verify timeouts
                Assertions.assertThat(overrideConfig.apiCallTimeout()).isPresent();
                Assertions.assertThat(overrideConfig.apiCallTimeout().get().getSeconds()).isEqualTo(10);

                Assertions.assertThat(overrideConfig.apiCallAttemptTimeout()).isPresent();
                Assertions.assertThat(overrideConfig.apiCallAttemptTimeout().get().getSeconds()).isEqualTo(5);

                // Verify SnsClient is created
                Assertions.assertThat(context.getBean(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME)).isInstanceOf(SnsClient.class);
            }
        );
    }

    /**
     * Test expected context with SNS disabled via property.
     */
    @Test
    void testExpectedContextWhenSNSDisabled() {
        this.contextRunner
            .withPropertyValues(
                "genie.notifications.sns.enabled=false"
            ).run(
                (context) -> {
                    Assertions.assertThat(context).doesNotHaveBean(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME);
                    // The override config should still be created
                    Assertions.assertThat(context).hasBean(AWSAutoConfiguration.SNS_CLIENT_OVERRIDE_CONFIG_BEAN_NAME);
                }
            );
    }

    /**
     * Test that the SNS client bean name matches the expected constant.
     */
    @Test
    void testSNSClientBeanName() {
        Assertions.assertThat(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME).isEqualTo("snsClient");
    }
}
