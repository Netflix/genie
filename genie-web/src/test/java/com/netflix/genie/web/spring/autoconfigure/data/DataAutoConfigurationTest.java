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
package com.netflix.genie.web.spring.autoconfigure.data;

import com.netflix.genie.web.data.repositories.jpa.JpaAgentConnectionRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaFileRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaJobRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaTagRepository;
import com.netflix.genie.web.data.services.jpa.JpaAgentConnectionPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaApplicationPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaClusterPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaCommandPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaFilePersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaJobPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaJobSearchServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaTagPersistenceServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link DataAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Disabled("For now this requires a lot of other auto configurations. Hard to isolate.")
class DataAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    DataAutoConfiguration.class
                )
            );

    /**
     * All the expected beans exist.
     */
    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(JpaAgentConnectionRepository.class);
                Assertions.assertThat(context).hasSingleBean(JpaApplicationRepository.class);
                Assertions.assertThat(context).hasSingleBean(JpaClusterRepository.class);
                Assertions.assertThat(context).hasSingleBean(JpaCommandRepository.class);
                Assertions.assertThat(context).hasSingleBean(JpaFileRepository.class);
                Assertions.assertThat(context).hasSingleBean(JpaJobRepository.class);
                Assertions.assertThat(context).hasSingleBean(JpaTagRepository.class);

                Assertions.assertThat(context).hasSingleBean(JpaAgentConnectionPersistenceServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaApplicationPersistenceServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaClusterPersistenceServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaCommandPersistenceServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaFilePersistenceServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaJobPersistenceServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaJobSearchServiceImpl.class);
                Assertions.assertThat(context).hasSingleBean(JpaTagPersistenceServiceImpl.class);
            }
        );
    }
}
