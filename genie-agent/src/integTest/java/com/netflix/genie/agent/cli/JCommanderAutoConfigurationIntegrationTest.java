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
package com.netflix.genie.agent.cli;

import com.beust.jcommander.JCommander;
import com.google.common.collect.Sets;
import com.netflix.genie.agent.configs.AgentAutoConfiguration;
import com.netflix.genie.agent.execution.ExecutionAutoConfiguration;
import com.netflix.genie.agent.execution.services.impl.ServicesAutoConfiguration;
import com.netflix.genie.agent.execution.services.impl.grpc.GRpcServicesAutoConfiguration;
import com.netflix.genie.agent.execution.statemachine.StateMachineAutoConfiguration;
import com.netflix.genie.agent.execution.statemachine.actions.StateMachineActionsAutoConfiguration;
import com.netflix.genie.agent.rpc.GRpcAutoConfiguration;
import com.netflix.genie.common.internal.configs.CommonServicesAutoConfiguration;
import com.netflix.genie.common.internal.configs.ProtoConvertersAutoConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test CLI beans in context.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = {
        JCommanderAutoConfiguration.class,
        CliAutoConfiguration.class,
        StateMachineAutoConfiguration.class,
        StateMachineActionsAutoConfiguration.class,
        ExecutionAutoConfiguration.class,
        GRpcServicesAutoConfiguration.class,
        GRpcAutoConfiguration.class,
        AgentAutoConfiguration.class,
        ServicesAutoConfiguration.class,
        ValidationAutoConfiguration.class,
        CommonServicesAutoConfiguration.class,
        ProtoConvertersAutoConfiguration.class
    }
)
// TODO: Do we want this to just use the application context runner and be a unit test or full blown integration test?
public class JCommanderAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Test creation of AgentArguments bean.
     */
    @Test
    public void globalAgentArguments() {
        Assert.assertNotNull(applicationContext.getBean(GlobalAgentArguments.class));
    }

    /**
     * Test creation of JCommander bean.
     */
    @Test
    public void jCommander() {
        Assert.assertNotNull(applicationContext.getBean(JCommander.class));
    }

    /**
     * Test creation of CommandFactory bean.
     */
    @Test
    public void commandFactory() {
        final CommandFactory factory = applicationContext.getBean(CommandFactory.class);
        Assert.assertNotNull(factory);

        final List<String> commandNames = Arrays.stream(CommandNames.class.getDeclaredFields())
            .filter(f -> Modifier.isStatic(f.getModifiers()))
            .filter(f -> f.getType() == String.class)
            .map((Field f) -> {
                try {
                    return (String) f.get(null);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to get value for field " + f.getName());
                }
            })
            .collect(Collectors.toList());

        Assert.assertEquals(
            Sets.newHashSet(commandNames),
            Sets.newHashSet(factory.getCommandNames())
        );

        for (String commandName : commandNames) {
            final AgentCommand command = factory.get(commandName);
            Assert.assertNotNull(command);
        }
    }

    /**
     * Test creation of ArgumentParser bean.
     */
    @Test
    public void argumentParser() {
        Assert.assertNotNull(applicationContext.getBean(ArgumentParser.class));
    }
}
