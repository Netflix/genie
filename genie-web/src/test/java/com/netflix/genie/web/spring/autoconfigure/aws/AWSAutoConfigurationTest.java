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

import com.amazonaws.retry.RetryPolicy;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextResourceLoaderAutoConfiguration;

/**
 * Tests for behavior of {@link AWSAutoConfiguration}.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class AWSAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                AWSAutoConfiguration.class,
                ContextCredentialsAutoConfiguration.class,
                ContextRegionProviderAutoConfiguration.class,
                ContextResourceLoaderAutoConfiguration.class,
                com.netflix.genie.common.internal.configs.AwsAutoConfiguration.class
            )
        )
        .withPropertyValues(
            "genie.retry.sns.noOfRetries=3",
            "genie.notifications.sns.enabled=true",
            "cloud.aws.credentials.useDefaultAwsCredentialsChain=true",
            "cloud.aws.region.auto=false",
            "cloud.aws.region.static=us-east-1",
            "cloud.aws.stack.auto=false",
            "spring.jmx.enabled=false",
            "spring.main.webApplicationType=none"
        );

    /**
     * Test expected context.
     */
    @Test
    public void testExpectedContext() {
        this.contextRunner.run(
            (context) -> {
                Assertions.assertThat(context).hasBean(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME);
                Assertions.assertThat(context).hasBean("JobNotificationsSNSClientRetryPolicy");
                Assertions.assertThat(context).hasBean("JobNotificationsSNSClientConfiguration");
                Assertions.assertThat(
                    context.getBean("JobNotificationsSNSClientRetryPolicy", RetryPolicy.class).getMaxErrorRetry()
                ).isEqualTo(3);
            }
        );
    }

    /**
     * Test expected context with SNS disabled via property.
     */
    @Test
    public void testExpectedContextWhenSNSDisabled() {
        this.contextRunner
            .withPropertyValues(
                "genie.notifications.sns.enabled=false"
            ).run(
            (context) -> {
                Assertions.assertThat(context).doesNotHaveBean(AWSAutoConfiguration.SNS_CLIENT_BEAN_NAME);
            }
        );
    }
}
