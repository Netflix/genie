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

import com.netflix.genie.agent.cli.ExitCode;
import com.netflix.genie.agent.cli.GenieAgentRunner;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests that ensure the app comes up correctly with default values.
 *
 * @author tgianos
 * @since 4.0.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = GenieAgentApplication.class)
public class GenieAgentApplicationSmokeTest {

    /**
     * Used for creating folders and files that are guaranteed to be removed upon test completion.
     */
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Autowired
    private ApplicationContext context;

    /**
     * Test to ensure the agent app can start up using the default configuration and run smoke tests against each of
     * the available top level commands.
     *
     * @throws Exception on any error
     */
    @Test
    public void smokeTestCommands() throws Exception {
        final GenieAgentRunner runner = this.context.getBean(GenieAgentRunner.class);

        // Test Help
        runner.run("help");
        Assert.assertThat(runner.getExitCode(), Matchers.is(ExitCode.SUCCESS.getCode()));

        // Test info
        runner.run("info", "--beans", "--env", "--properties", "--state-machine");
        Assert.assertThat(runner.getExitCode(), Matchers.is(ExitCode.SUCCESS.getCode()));
    }
}
