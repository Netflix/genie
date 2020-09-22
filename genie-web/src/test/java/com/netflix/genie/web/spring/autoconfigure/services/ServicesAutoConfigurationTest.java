/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.services;

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import com.netflix.genie.web.properties.ExponentialBackOffTriggerProperties;
import com.netflix.genie.web.properties.JobsActiveLimitProperties;
import com.netflix.genie.web.properties.JobsForwardingProperties;
import com.netflix.genie.web.properties.JobsLocationsProperties;
import com.netflix.genie.web.properties.JobsMaxProperties;
import com.netflix.genie.web.properties.JobsMemoryProperties;
import com.netflix.genie.web.properties.JobsUsersProperties;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.impl.LocalFileSystemAttachmentServiceImpl;
import com.netflix.genie.web.services.impl.S3AttachmentServiceImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.net.URI;

/**
 * Unit Tests for {@link ServicesAutoConfiguration} class.
 *
 * @author amsharma
 * @since 3.0.0
 */
class ServicesAutoConfigurationTest {
    //TODO update this test class to use ContextRunner, like the rest of configuration tests

    private ServicesAutoConfiguration servicesAutoConfiguration;

    @BeforeEach
    void setUp() {
        this.servicesAutoConfiguration = new ServicesAutoConfiguration();
    }

    @Test
    void canGetJobPropertiesBean() {
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobsProperties(
                    Mockito.mock(JobsForwardingProperties.class),
                    Mockito.mock(JobsLocationsProperties.class),
                    Mockito.mock(JobsMaxProperties.class),
                    Mockito.mock(JobsMemoryProperties.class),
                    Mockito.mock(JobsUsersProperties.class),
                    Mockito.mock(ExponentialBackOffTriggerProperties.class),
                    Mockito.mock(JobsActiveLimitProperties.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetJobDirectoryServerServiceBean() {
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobDirectoryServerService(
                    Mockito.mock(ResourceLoader.class),
                    Mockito.mock(DataServices.class),
                    Mockito.mock(AgentFileStreamService.class),
                    Mockito.mock(ArchivedJobService.class),
                    Mockito.mock(MeterRegistry.class),
                    Mockito.mock(AgentRoutingService.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetS3AttachmentServiceServiceBean() throws IOException {

        final AttachmentServiceProperties properties = new AttachmentServiceProperties();

        Assertions
            .assertThat(
                this.servicesAutoConfiguration.attachmentService(
                    Mockito.mock(S3ClientFactory.class),
                    properties,
                    Mockito.mock(MeterRegistry.class)
                )
            )
            .isInstanceOf(LocalFileSystemAttachmentServiceImpl.class);

        properties.setLocationPrefix(URI.create("s3://foo/bar/genie/attachments"));

        Assertions
            .assertThat(
                this.servicesAutoConfiguration.attachmentService(
                    Mockito.mock(S3ClientFactory.class),
                    properties,
                    Mockito.mock(MeterRegistry.class)
                )
            )
            .isInstanceOf(S3AttachmentServiceImpl.class);
    }
}
