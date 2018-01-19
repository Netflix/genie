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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Command to print diagnostic information such as environment variables, beans, etc.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class InfoCommand implements AgentCommand {

    private final InfoCommandArguments infoCommandArguments;
    private final ConfigurableApplicationContext applicationContext;

    InfoCommand(
        final InfoCommandArguments infoCommandArguments,
        final ConfigurableApplicationContext applicationContext
    ) {
        this.infoCommandArguments = infoCommandArguments;
        this.applicationContext = applicationContext;
    }

    @Override
    public void run() {

        if (infoCommandArguments.getIncludeBeans()) {
            System.out.println("Beans in context: " + applicationContext.getBeanDefinitionCount());
            final String[] beanNames = applicationContext.getBeanDefinitionNames();
            for (String beanName : beanNames) {

                final BeanDefinition beanDefinition = applicationContext.getBeanFactory().getBeanDefinition(beanName);
                final String beanClass = beanDefinition.getBeanClassName();

                final String description = new StringBuilder()
                    .append(beanDefinition.isLazyInit() ? "lazy" : "eager")
                    .append(beanDefinition.isPrototype() ? ", prototype" : "")
                    .append(beanDefinition.isSingleton() ? ", singleton" : "")
                    .toString();

                System.out.println(
                    String.format(
                        "  - %s (%s) [%s]",
                        beanName,
                        beanClass,
                        description
                    )
                );
            }
        }

        if (infoCommandArguments.getIncludeEnvironment()) {
            System.out.println("Environment variables:");
            for (Map.Entry<String, String> envEntry : System.getenv().entrySet()) {
                System.out.println(
                    String.format(
                        "  - %s=%s",
                        envEntry.getKey(),
                        envEntry.getValue()
                    )
                );
            }
        }

        if (infoCommandArguments.isIncludeProperties()) {
            System.out.println("Properties:");
            for (String propertyName : System.getProperties().stringPropertyNames()) {
                System.out.println(
                    String.format(
                        "  - %s=%s",
                        propertyName,
                        System.getProperty(propertyName)
                    )
                );
            }
        }
    }

    @Component
    @Parameters(commandNames = CommandNames.INFO, commandDescription = "Print agent and environment information")
    @Getter
    static class InfoCommandArguments implements AgentCommandArguments {
        @Parameter(names = {"--beans"}, description = "Print beans")
        private Boolean includeBeans = true;
        @Parameter(names = {"--env"}, description = "Print environment variables")
        private Boolean includeEnvironment = true;
        @Parameter(names = {"--properties"}, description = "Print properties")
        private boolean includeProperties = true;

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return InfoCommand.class;
        }
    }
}
