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

import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.JobDirectoryManifestService;
import com.netflix.genie.common.internal.services.impl.FileSystemJobArchiverImpl;
import com.netflix.genie.common.internal.services.impl.S3JobArchiverImpl;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration;
import org.springframework.cloud.aws.autoconfigure.context.ContextResourceLoaderAutoConfiguration;

/**
 * Tests for behavior of {@link CommonServicesAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class CommonServicesAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                CommonServicesAutoConfiguration.class
            )
        )
        .withPropertyValues(
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
                Assertions.assertThat(context).hasSingleBean(FileSystemJobArchiverImpl.class);
                Assertions.assertThat(context).hasSingleBean(JobArchiver.class);
                Assertions.assertThat(context).hasSingleBean(JobArchiveService.class);
            }
        );
    }

    /**
     * Make sure when AWS configuration is involved it gives the right configuration.
     */
    @Test
    public void testExpectedContextWithAws() {
        this.contextRunner
            .withPropertyValues(
                "cloud.aws.credentials.useDefaultAwsCredentialsChain=true",
                "cloud.aws.region.auto=false",
                "cloud.aws.region.static=us-east-1",
                "cloud.aws.stack.auto=false"
            )
            .withConfiguration(
                AutoConfigurations.of(
                    ContextCredentialsAutoConfiguration.class,
                    ContextRegionProviderAutoConfiguration.class,
                    ContextResourceLoaderAutoConfiguration.class,
                    AwsAutoConfiguration.class
                )
            )
            .run(
                (context) -> {
                    Assertions.assertThat(context).hasSingleBean(FileSystemJobArchiverImpl.class);
                    Assertions.assertThat(context).hasSingleBean(S3JobArchiverImpl.class);
                    Assertions.assertThat(context).hasSingleBean(JobArchiveService.class);

                    // TODO: Find a way to test the order
                    Assertions
                        .assertThat(context)
                        .getBeans(JobArchiver.class)
                        .size()
                        .isEqualTo(2);
                }
            );
    }

    /**
     * Make JobDirectoryManifestService beans is configured as expected.
     */
    @Test
    public void testJobDirectoryManifestService() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(JobDirectoryManifestService.class);
            }
        );
    }

    /**
     * Make sure DirectoryManifest.Factory bean is configured as expected.
     */
    @Test
    public void testDirectoryManifestFactory() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(DirectoryManifest.Factory.class);
            }
        );
    }

    /**
     * Make JobDirectoryManifestService beans are configured as expected.
     */
    @Test
    public void testDirectoryManifestFilter() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(DirectoryManifest.Filter.class);
            }
        );
    }
}
