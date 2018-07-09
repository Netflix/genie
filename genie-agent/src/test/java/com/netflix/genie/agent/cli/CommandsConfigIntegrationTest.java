package com.netflix.genie.agent.cli;

import com.beust.jcommander.JCommander;
import com.google.common.collect.Sets;
import com.netflix.genie.GenieAgentApplication;
import com.netflix.genie.test.categories.IntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
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
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = GenieAgentApplication.class)
public class CommandsConfigIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Test creation of AgentArguments bean.
     *
     * @throws Exception for error
     */
    @Test
    public void globalAgentArguments() throws Exception {
        Assert.assertNotNull(applicationContext.getBean(GlobalAgentArguments.class));
    }

    /**
     * Test creation of JCommander bean.
     *
     * @throws Exception for error
     */
    @Test
    public void jCommander() throws Exception {
        Assert.assertNotNull(applicationContext.getBean(JCommander.class));
    }

    /**
     * Test creation of CommandFactory bean.
     *
     * @throws Exception for error
     */
    @Test
    public void commandFactory() throws Exception {
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
     *
     * @throws Exception for error
     */
    @Test
    public void argumentParser() throws Exception {
        Assert.assertNotNull(applicationContext.getBean(ArgumentParser.class));
    }
}
