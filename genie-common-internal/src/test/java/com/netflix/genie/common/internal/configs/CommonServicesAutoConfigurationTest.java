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

import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.common.internal.services.impl.FileSystemJobArchiverImpl;
import com.netflix.genie.common.internal.services.impl.S3JobArchiverImpl;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import io.awspring.cloud.autoconfigure.context.ContextCredentialsAutoConfiguration;
import io.awspring.cloud.autoconfigure.context.ContextRegionProviderAutoConfiguration;
import io.awspring.cloud.autoconfigure.context.ContextResourceLoaderAutoConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for behavior of {@link CommonServicesAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class CommonServicesAutoConfigurationTest {
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
    void testExpectedContext() {
        this.contextRunner.run(
            (context) -> {
                Assertions.assertThat(context).hasSingleBean(FileSystemJobArchiverImpl.class);
                Assertions.assertThat(context).hasSingleBean(JobArchiver.class);
                Assertions.assertThat(context).hasSingleBean(JobArchiveService.class);
                Assertions.assertThat(context).hasSingleBean(PropertiesMapCache.Factory.class);
            }
        );
    }

    /**
     * Make sure when AWS configuration is involved it gives the right configuration.
     */
    @Test
    void testExpectedContextWithAws() {
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
                    Assertions.assertThat(context).hasSingleBean(PropertiesMapCache.Factory.class);

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
    void testJobDirectoryManifestService() {
        this.contextRunner.run(
            context -> Assertions.assertThat(context).hasSingleBean(JobDirectoryManifestCreatorService.class)
        );
    }

    /**
     * Make JobDirectoryManifest cache bean is configured as expected.
     */
    @Test
    void testJobDirectoryManifestCache() {
        this.contextRunner.run(
            context -> Assertions.assertThat(context).getBean("jobDirectoryManifestCache").isNotNull()
        );
    }

    /**
     * Make sure DirectoryManifest.Factory bean is configured as expected.
     */
    @Test
    void testDirectoryManifestFactory() {
        this.contextRunner.run(
            context -> Assertions.assertThat(context).hasSingleBean(DirectoryManifest.Factory.class)
        );
    }

    /**
     * Make JobDirectoryManifestService beans are configured as expected.
     */
    @Test
    void testDirectoryManifestFilter() {
        this.contextRunner.run(
            context -> Assertions.assertThat(context).hasSingleBean(DirectoryManifest.Filter.class)
        );
    }
}
