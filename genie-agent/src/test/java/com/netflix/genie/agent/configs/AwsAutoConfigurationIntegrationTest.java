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
package com.netflix.genie.agent.configs;

import com.amazonaws.regions.AwsRegionProvider;
import com.netflix.genie.agent.aws.s3.S3ClientFactory;
import com.netflix.genie.agent.aws.s3.S3ProtocolResolver;
import com.netflix.genie.agent.aws.s3.S3ProtocolResolverRegistrar;
import com.netflix.genie.agent.execution.services.ArchivalService;
import com.netflix.genie.test.categories.IntegrationTest;
import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextResourceLoaderAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.properties.AwsS3ResourceLoaderProperties;
import org.springframework.cloud.aws.context.support.io.SimpleStorageProtocolResolverConfigurer;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ProtocolResolver;

import java.util.Collection;

/**
 * Tests for behavior of {@link AwsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(IntegrationTest.class)
public class AwsAutoConfigurationIntegrationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                ContextCredentialsAutoConfiguration.class,
                ContextRegionProviderAutoConfiguration.class,
                ContextResourceLoaderAutoConfiguration.class,
                AwsAutoConfiguration.class
            )
        )
        .withPropertyValues(
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
                Assertions.assertThat(context).hasSingleBean(AwsRegionProvider.class);
                Assertions.assertThat(context).hasSingleBean(S3ClientFactory.class);
                Assertions.assertThat(context).hasSingleBean(ArchivalService.class);
                Assertions.assertThat(context).hasSingleBean(AwsS3ResourceLoaderProperties.class);
                Assertions.assertThat(context).hasSingleBean(S3ProtocolResolver.class);
                Assertions.assertThat(context).hasSingleBean(S3ProtocolResolverRegistrar.class);

                // Verify that Spring Cloud AWS still would try to register their S3 protocol resolver
                Assertions.assertThat(context).hasSingleBean(SimpleStorageProtocolResolverConfigurer.class);

                // And Make sure we ripped out the one from Spring Cloud AWS and put ours in instead
                if (context instanceof AbstractApplicationContext) {
                    final AbstractApplicationContext aac = (AbstractApplicationContext) context;
                    final Collection<ProtocolResolver> protocolResolvers = aac.getProtocolResolvers();
                    Assert.assertThat(
                        protocolResolvers,
                        Matchers.contains(context.getBean(S3ProtocolResolver.class))
                    );
                    Assert.assertThat(
                        protocolResolvers,
                        Matchers.not(
                            Matchers.hasItem(
                                Matchers.isA(SimpleStorageProtocolResolver.class)
                            )
                        )
                    );
                }
            }
        );
    }
}
