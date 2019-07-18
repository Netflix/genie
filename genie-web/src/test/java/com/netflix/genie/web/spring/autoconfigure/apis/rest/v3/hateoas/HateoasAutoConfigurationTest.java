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

import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobExecutionResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobMetadataResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobRequestResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobSearchResultResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.RootResourceAssembler;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for sub auto configuration {@link HateoasAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class HateoasAutoConfigurationTest {
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
    public void expectedHateoasBeansAreCreated() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(ApplicationResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(ClusterResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(CommandResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobExecutionResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobMetadataResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobRequestResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(JobSearchResultResourceAssembler.class);
                Assertions.assertThat(context).hasSingleBean(RootResourceAssembler.class);
            }
        );
    }
}
