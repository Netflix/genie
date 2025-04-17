/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.common.internal.configs;

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.aws.s3.S3ProtocolResolver;
import com.netflix.genie.common.internal.aws.s3.S3ProtocolResolverRegistrar;
import com.netflix.genie.common.internal.aws.s3.S3TransferManagerFactory;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.impl.S3JobArchiverImpl;
import io.awspring.cloud.autoconfigure.core.RegionProperties;
import io.awspring.cloud.autoconfigure.s3.properties.S3Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ProtocolResolver;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.Collection;

/**
 * Tests for behavior of {@link AwsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class AwsAutoConfigurationTest {

    @Configuration
    static class TestConfiguration {
        @Bean
        public AwsCredentialsProvider credentialsProvider() {
            return DefaultCredentialsProvider.create();
        }

        @Bean
        public RegionProperties regionProperties() {
            RegionProperties properties = new RegionProperties();
            properties.setStatic("us-east-1");
            return properties;
        }

        @Bean
        public S3Properties s3Properties() {
            return new S3Properties();
        }
    }

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                AwsAutoConfiguration.class
            )
        )
        .withUserConfiguration(TestConfiguration.class)
        .withPropertyValues(
            "spring.cloud.aws.credentials.use-default-aws-credentials-chain=true",
            "spring.cloud.aws.region.auto=false",
            "spring.cloud.aws.region.static=us-east-1",
            "spring.jmx.enabled=false",
            "spring.main.web-application-type=none"
        )
        .withPropertyValues("spring.main.allow-bean-definition-overriding=true");

    /**
     * Test expected context.
     */
    @Test
    void testExpectedContext() {
        this.contextRunner.run(
            (context) -> {
                // Check for our specific beans
                Assertions.assertThat(context).hasSingleBean(S3ClientFactory.class);
                Assertions.assertThat(context).hasSingleBean(S3ProtocolResolver.class);
                Assertions.assertThat(context).hasSingleBean(S3ProtocolResolverRegistrar.class);
                Assertions.assertThat(context).hasSingleBean(S3TransferManagerFactory.class);
                Assertions.assertThat(context).hasSingleBean(S3JobArchiverImpl.class);
                Assertions.assertThat(context).hasSingleBean(JobArchiver.class);

                // And Make sure we ripped out the one from Spring Cloud AWS and put ours in instead
                if (context instanceof AbstractApplicationContext) {
                    final AbstractApplicationContext aac = (AbstractApplicationContext) context;
                    final Collection<ProtocolResolver> protocolResolvers = aac.getProtocolResolvers();
                    Assertions.assertThat(protocolResolvers).contains(context.getBean(S3ProtocolResolver.class));
                }
            }
        );
    }
}
