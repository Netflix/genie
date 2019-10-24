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
package com.netflix.genie.web.spring.autoconfigure.agent.inspectors;

import com.netflix.genie.web.agent.inspectors.AgentMetadataInspector;
import com.netflix.genie.web.agent.inspectors.impl.BlacklistedVersionAgentMetadataInspector;
import com.netflix.genie.web.agent.inspectors.impl.MinimumVersionAgentMetadataInspector;
import com.netflix.genie.web.agent.inspectors.impl.RejectAllJobsAgentMetadataInspector;
import com.netflix.genie.web.agent.inspectors.impl.WhitelistedVersionAgentMetadataInspector;
import com.netflix.genie.web.properties.AgentFilterProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Tests for {@link AgentInspectorsAutoConfiguration}.
 *
 * @author mprimi
 * @since 4.0.0
 */
class AgentInspectorsAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    AgentInspectorsAutoConfiguration.class
                )
            );

    /**
     * Test expected context when configuration is enabled.
     */
    @Test
    void testContextIfEnabled() {
        this.contextRunner
            .withPropertyValues(
                "genie.agent.filter.enabled=true"
            )
            .run(
                (context) -> {
                    Assertions.assertThat(context).hasSingleBean(AgentFilterProperties.class);
                    Assertions.assertThat(context).getBeans(AgentMetadataInspector.class).hasSize(4);
                    Assertions.assertThat(context).hasSingleBean(WhitelistedVersionAgentMetadataInspector.class);
                    Assertions.assertThat(context).hasSingleBean(BlacklistedVersionAgentMetadataInspector.class);
                    Assertions.assertThat(context).hasSingleBean(MinimumVersionAgentMetadataInspector.class);
                    Assertions.assertThat(context).hasSingleBean(RejectAllJobsAgentMetadataInspector.class);
                }
            );
    }

    /**
     * Test expected context when configuration is disabled.
     */
    @Test
    void testContextIfDisabled() {
        this.contextRunner
            .withPropertyValues(
                "genie.agent.filter.enabled=nope"
            )
            .run(
                (context) -> {
                    Assertions.assertThat(context).doesNotHaveBean(AgentFilterProperties.class);
                    Assertions.assertThat(context).doesNotHaveBean(AgentMetadataInspector.class);
                }
            );
    }

    /**
     * Test expected context when configuration is not explicitly enabled or disabled.
     */
    @Test
    void testContextIfUnspecified() {
        this.contextRunner
            .withPropertyValues(
                "genie.some.other.property=true"
            )
            .run(
                (context) -> {
                    Assertions.assertThat(context).doesNotHaveBean(AgentFilterProperties.class);
                    Assertions.assertThat(context).doesNotHaveBean(AgentMetadataInspector.class);
                }
            );
    }
}
