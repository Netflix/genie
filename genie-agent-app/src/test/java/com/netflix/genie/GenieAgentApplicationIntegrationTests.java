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

import com.netflix.genie.agent.cli.GenieAgentRunner;
import com.netflix.genie.test.categories.IntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = GenieAgentApplication.class)
public class GenieAgentApplicationIntegrationTests {

    @Autowired
    private ApplicationContext context;

    /**
     * Test to ensure the agent app can start up using the default configuration and print help.
     *
     * @throws Exception on any error
     */
    @Test
    public void canStartup() throws Exception {
        final GenieAgentRunner runner = this.context.getBean(GenieAgentRunner.class);
        runner.run("help");
        Assert.assertThat(runner.getExitCode(), Matchers.is(0));
    }
}
