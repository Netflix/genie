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

import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.test.categories.IntegrationTest;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for behavior of {@link ProtoConvertersAutoConfiguration}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Category(IntegrationTest.class)
public class ProtoConvertersAutoConfigurationIntegrationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                ProtoConvertersAutoConfiguration.class
            )
        );

    /**
     * Test expected context.
     */
    @Test
    public void testExpectedContext() {
        this.contextRunner.run(
            (context) -> {
                Assertions.assertThat(context).hasSingleBean(JobServiceProtoConverter.class);
                Assertions.assertThat(context).hasSingleBean(JobDirectoryManifestProtoConverter.class);
            }
        );
    }
}
