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

import com.beust.jcommander.Parameters;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory to create a (lazy) instance of the requested AgentCommand.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class CommandFactory {
    private final List<AgentCommandArguments> agentCommandArgumentsBeans;
    private final ApplicationContext applicationContext;
    private final Map<String, Class<? extends AgentCommand>> commandMap = Maps.newHashMap();

    CommandFactory(
        final List<AgentCommandArguments> agentCommandArguments,
        final ApplicationContext applicationContext
    ) {
        this.agentCommandArgumentsBeans = agentCommandArguments;
        this.applicationContext = applicationContext;

        agentCommandArguments.forEach(
            commandArgs -> {
                Sets.newHashSet(commandArgs.getClass().getAnnotation(Parameters.class).commandNames()).forEach(
                    commandName -> {
                        commandMap.put(commandName, commandArgs.getConsumerClass());
                    }
                );
            }
        );
    }

    AgentCommand get(final String requestedCommandName) {

        final Class<? extends AgentCommand> commandClass = commandMap.get(requestedCommandName);
        final AgentCommand commandInstance;

        if (commandClass == null) {
            log.error("Command not found: {}", requestedCommandName);
            commandInstance = null;
        } else {
            commandInstance = applicationContext.getAutowireCapableBeanFactory().getBean(commandClass);
        }

        return commandInstance;
    }

    Set<String> getCommandNames() {
        return commandMap.keySet();
    }
}
