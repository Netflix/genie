/*
 *
 *  Copyright 2022 Netflix, Inc.
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
package com.netflix.genie.web.properties;

import com.netflix.genie.common.internal.dtos.ComputeResources;
import com.netflix.genie.common.internal.dtos.Image;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.util.unit.DataSize;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link JobResolutionProperties}.
 *
 * @author tgianos
 * @since 4.3.0
 */
class JobResolutionPropertiesTest {

    private MockEnvironment environment;

    @BeforeEach
    void setup() {
        this.environment = new MockEnvironment();
    }

    @Test
    void defaultsSetProperly() {
        final JobResolutionProperties properties = new JobResolutionProperties(this.environment);
        Assertions
            .assertThat(properties.getDefaultComputeResources())
            .isNotNull();
        final ComputeResources computeResources = properties.getDefaultComputeResources();
        Assertions.assertThat(computeResources.getCpu()).contains(1);
        Assertions.assertThat(computeResources.getGpu()).contains(0);
        Assertions.assertThat(computeResources.getMemoryMb()).contains(1500);
        Assertions.assertThat(computeResources.getDiskMb()).contains(DataSize.ofGigabytes(10L).toMegabytes());
        Assertions
            .assertThat(computeResources.getNetworkMbps())
            .contains(DataSize.ofMegabytes(1_250L).toMegabytes() * 8);
        Assertions
            .assertThat(properties.getDefaultImages())
            .isNotNull()
            .isEmpty();
    }

    @Test
    void computeResourcesOverriddenProperly() {
        this.environment
            .withProperty("genie.services.resolution.defaults.runtime.resources.cpu", "7")
            .withProperty("genie.services.resolution.defaults.runtime.resources.gpu", "1")
            .withProperty("genie.services.resolution.defaults.runtime.resources.memory", "1024")
            .withProperty("genie.services.resolution.defaults.runtime.resources.disk", "100GB")
            .withProperty("genie.services.resolution.defaults.runtime.resources.network", "15GB");
        final JobResolutionProperties properties = new JobResolutionProperties(this.environment);
        Assertions
            .assertThat(properties.getDefaultComputeResources())
            .isNotNull();
        final ComputeResources computeResources = properties.getDefaultComputeResources();
        Assertions.assertThat(computeResources.getCpu()).contains(7);
        Assertions.assertThat(computeResources.getGpu()).contains(1);
        Assertions.assertThat(computeResources.getMemoryMb()).contains(1024);
        Assertions.assertThat(computeResources.getDiskMb()).contains(DataSize.ofGigabytes(100L).toMegabytes());
        Assertions
            .assertThat(computeResources.getNetworkMbps())
            .contains(DataSize.ofGigabytes(15L).toMegabytes() * 8);
        Assertions
            .assertThat(properties.getDefaultImages())
            .isNotNull()
            .isEmpty();
    }

    @Test
    void defaultImagesOverriddenProperly() {
        this.environment
            .withProperty("genie.services.resolution.defaults.runtime.images.genie.name", "genie-agent")
            .withProperty("genie.services.resolution.defaults.runtime.images.genie.tag", "4.3.0")
            .withProperty("genie.services.resolution.defaults.runtime.images.genie.arguments[0]", "hi")
            .withProperty("genie.services.resolution.defaults.runtime.images.genie.arguments[1]", "bye")
            .withProperty("genie.services.resolution.defaults.runtime.images.python.tag", "3.10.0")
            .withProperty("genie.services.resolution.defaults.runtime.images.python.arguments[0]", "pip")
            .withProperty("genie.services.resolution.defaults.runtime.images.python.arguments[1]", "freeze");
        final JobResolutionProperties properties = new JobResolutionProperties(this.environment);
        Assertions
            .assertThat(properties.getDefaultComputeResources())
            .isNotNull();
        final Map<String, Image> defaultImages = properties.getDefaultImages();
        Assertions
            .assertThat(defaultImages)
            .isNotNull()
            .containsKey("genie")
            .containsKey("python");
        final Image genieAgent = defaultImages.get("genie");
        Assertions.assertThat(genieAgent).isNotNull();
        Assertions.assertThat(genieAgent.getName()).contains("genie-agent");
        Assertions.assertThat(genieAgent.getTag()).contains("4.3.0");
        Assertions.assertThat(genieAgent.getArguments()).isNotNull().isEqualTo(List.of("hi", "bye"));
        final Image python = defaultImages.get("python");
        Assertions.assertThat(python).isNotNull();
        Assertions.assertThat(python.getName()).isEmpty();
        Assertions.assertThat(python.getTag()).contains("3.10.0");
        Assertions.assertThat(python.getArguments()).isNotNull().isEqualTo(List.of("pip", "freeze"));
    }

    @Test
    void refreshWorksProperly() {
        final JobResolutionProperties properties = new JobResolutionProperties(this.environment);
        Assertions
            .assertThat(properties.getDefaultComputeResources())
            .isNotNull();
        ComputeResources computeResources = properties.getDefaultComputeResources();
        Assertions.assertThat(computeResources.getCpu()).contains(1);
        this.environment.withProperty("genie.services.resolution.defaults.runtime.resources.cpu", "3");
        properties.refresh();
        computeResources = properties.getDefaultComputeResources();
        Assertions.assertThat(computeResources.getCpu()).contains(3);
    }
}
