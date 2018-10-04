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
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Auto configuration for configuring {@link JCommander} to parse CLI arguments of the Agent.
 *
 * @author mprimi
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class JCommanderAutoConfiguration {

    /**
     * Provide a {@link GlobalAgentArguments} bean if one hasn't already been provided.
     *
     * @return A {@link GlobalAgentArguments} instance
     */
    @Bean
    @ConditionalOnMissingBean(GlobalAgentArguments.class)
    public GlobalAgentArguments globalAgentArguments() {
        return new GlobalAgentArguments();
    }

    /**
     * Provide a {@link JCommander} instance if one hasn't already been provided.
     *
     * @param globalAgentArguments  The global command arguments to use
     * @param agentCommandArguments An command argument beans in the environment that should also be used in addition
     *                              to the global command arguments
     * @return A {@link JCommander} instance
     */
    @Bean
    @ConditionalOnMissingBean(JCommander.class)
    public JCommander jCommander(
        final GlobalAgentArguments globalAgentArguments,
        final List<AgentCommandArguments> agentCommandArguments
    ) {
        final JCommander.Builder jCommanderBuilder = JCommander.newBuilder()
            .addObject(globalAgentArguments)
            .acceptUnknownOptions(false);

        agentCommandArguments.forEach(jCommanderBuilder::addCommand);

        return jCommanderBuilder.build();
    }

    /**
     * Provide a command factory if one hasn't already been provided.
     *
     * @param agentCommandArguments Any agent command argument implementations that are in the application context
     * @param applicationContext    The Spring application context
     * @return A {@link CommandFactory} instance
     */
    @Bean
    @ConditionalOnMissingBean(CommandFactory.class)
    public CommandFactory commandFactory(
        final List<AgentCommandArguments> agentCommandArguments,
        final ApplicationContext applicationContext
    ) {
        return new CommandFactory(agentCommandArguments, applicationContext);
    }

    /**
     * Provide an argument parser instance if one hasn't already been defined.
     *
     * @param jCommander     The JCommander instance to use
     * @param commandFactory The command factory instance to use
     * @return An {@link ArgumentParser} instance
     */
    @Bean
    @ConditionalOnMissingBean(ArgumentParser.class)
    public ArgumentParser argumentParser(
        final JCommander jCommander,
        final CommandFactory commandFactory
    ) {
        return new ArgumentParser(
            jCommander,
            commandFactory
        );
    }

}
