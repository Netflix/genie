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
package com.netflix.genie;

import brave.Tracer;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.sampler.Sampler;
import com.netflix.genie.agent.cli.CliAutoConfiguration;
import com.netflix.genie.agent.cli.ExitCode;
import com.netflix.genie.agent.cli.GenieAgentRunner;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Tests that ensure the app comes up correctly with default values.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(
    classes = GenieAgentApplicationSmokeTest.TestConfig.class, // 只使用我们自己的配置
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class GenieAgentApplicationSmokeTest {

    @Configuration
    @Import(CliAutoConfiguration.class) // 导入CliAutoConfiguration
    static class TestConfig {
        @Bean
        public Tracing tracing() {
            return Tracing.newBuilder()
                .localServiceName("genie-agent-test")
                .sampler(Sampler.NEVER_SAMPLE)
                .build();
        }

        @Bean
        public Tracer tracer(Tracing tracing) {
            return tracing.tracer();
        }

        @Bean
        public CurrentTraceContext currentTraceContext(Tracing tracing) {
            return tracing.currentTraceContext();
        }
    }

    @Autowired
    private GenieAgentRunner genieAgentRunner;

    @Test
    public void smokeTestCommands() throws Exception {
        // Test Help
        genieAgentRunner.run("help");
        Assertions.assertThat(genieAgentRunner.getExitCode()).isEqualTo(ExitCode.SUCCESS.getCode());

        // Test info
        genieAgentRunner.run("info", "--beans", "--env", "--properties", "--state-machine");
        Assertions.assertThat(genieAgentRunner.getExitCode()).isEqualTo(ExitCode.SUCCESS.getCode());
    }
}
