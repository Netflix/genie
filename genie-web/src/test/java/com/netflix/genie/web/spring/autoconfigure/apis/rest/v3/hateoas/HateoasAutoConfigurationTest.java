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
package com.netflix.genie.web.spring.autoconfigure.apis.rest.v3.hateoas;

import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.CommandModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.EntityModelAssemblers;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobExecutionModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobMetadataModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobRequestModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobSearchResultModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.RootModelAssembler;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for sub auto configuration {@link HateoasAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class HateoasAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    HateoasAutoConfiguration.class
                )
            );

    /**
     * Make sure the configuration creates all the expected beans.
     */
    @Test
    void expectedHateoasBeansAreCreated() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(ApplicationModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(ClusterModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(CommandModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobExecutionModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobMetadataModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobRequestModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobSearchResultModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(RootModelAssembler.class);
                Assertions.assertThat(context).hasSingleBean(EntityModelAssemblers.class);
            }
        );
    }
}
