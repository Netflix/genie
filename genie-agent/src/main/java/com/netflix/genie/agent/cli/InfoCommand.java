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
import com.netflix.genie.agent.AgentMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;

import java.util.Map;
import java.util.Set;

/**
 * Command to print diagnostic information such as environment variables, beans, etc.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class InfoCommand implements AgentCommand {

    private final InfoCommandArguments infoCommandArguments;
    private final ConfigurableApplicationContext applicationContext;
    private final AgentMetadata agentMetadata;

    InfoCommand(
        final InfoCommandArguments infoCommandArguments,
        final ConfigurableApplicationContext applicationContext,
        final AgentMetadata agentMetadata
    ) {
        this.infoCommandArguments = infoCommandArguments;
        this.applicationContext = applicationContext;
        this.agentMetadata = agentMetadata;
    }

    @Override
    public ExitCode run() {

        final StringBuilder messageBuilder = new StringBuilder();

        messageBuilder
            .append("Agent info:")
            .append(System.lineSeparator())
            .append("  version:")
            .append(agentMetadata.getAgentVersion())
            .append(System.lineSeparator())
            .append("  host:")
            .append(agentMetadata.getAgentHostName())
            .append(System.lineSeparator())
            .append("  pid:")
            .append(agentMetadata.getAgentPid())
            .append(System.lineSeparator());

        messageBuilder
            .append("Active profiles:")
            .append(System.lineSeparator());

        for (String profileName : applicationContext.getEnvironment().getActiveProfiles()) {
            messageBuilder
                .append("  - ")
                .append(profileName)
                .append(System.lineSeparator());
        }

        messageBuilder
            .append("Default profiles:")
            .append(System.lineSeparator());

        for (String profileName : applicationContext.getEnvironment().getDefaultProfiles()) {
            messageBuilder
                .append("  - ")
                .append(profileName)
                .append(System.lineSeparator());
        }

        if (infoCommandArguments.getIncludeBeans()) {
            messageBuilder
                .append("Beans in context: ")
                .append(applicationContext.getBeanDefinitionCount())
                .append(System.lineSeparator());

            final String[] beanNames = applicationContext.getBeanDefinitionNames();
            for (String beanName : beanNames) {

                final BeanDefinition beanDefinition = applicationContext.getBeanFactory().getBeanDefinition(beanName);
                final String beanClass = beanDefinition.getBeanClassName();

                final String description = new StringBuilder()
                    .append(beanDefinition.isLazyInit() ? "lazy" : "eager")
                    .append(beanDefinition.isPrototype() ? ", prototype" : "")
                    .append(beanDefinition.isSingleton() ? ", singleton" : "")
                    .toString();

                messageBuilder
                    .append(
                        String.format(
                            "  - %s (%s) [%s]",
                            beanName,
                            beanClass == null ? "?" : beanClass,
                            description
                        )
                    )
                    .append(System.lineSeparator());
            }
        }

        if (infoCommandArguments.getIncludeEnvironment()) {

            final Set<Map.Entry<String, Object>> envEntries =
                applicationContext.getEnvironment().getSystemEnvironment().entrySet();

            messageBuilder
                .append("Environment variables: ")
                .append(envEntries.size())
                .append(System.lineSeparator());

            for (Map.Entry<String, Object> envEntry : envEntries) {
                messageBuilder
                    .append(
                        String.format(
                            "  - %s=%s",
                            envEntry.getKey(),
                            envEntry.getValue()
                        )
                    )
                    .append(System.lineSeparator());
            }
        }

        if (infoCommandArguments.isIncludeProperties()) {

            final Set<Map.Entry<String, Object>> properties =
                applicationContext.getEnvironment().getSystemProperties().entrySet();

            messageBuilder
                .append("Properties: ")
                .append(properties.size())
                .append(System.lineSeparator());
            for (Map.Entry<String, Object> property : properties) {
                messageBuilder
                    .append(
                        String.format(
                            "  - %s=%s",
                            property.getKey(),
                            property.getValue()
                        )
                    )
                    .append(System.lineSeparator());
            }

            final PropertySources propertySources = applicationContext.getEnvironment().getPropertySources();
            messageBuilder
                .append("Property sources: ")
                .append(System.lineSeparator());
            for (PropertySource<?> propertySource : propertySources) {
                messageBuilder
                    .append(
                        String.format(
                            "  - %s (%s)",
                            propertySource.getName(),
                            propertySource.getClass().getSimpleName()
                        )
                    )
                    .append(System.lineSeparator());
            }
        }

        System.out.println(messageBuilder.toString());

        return ExitCode.SUCCESS;
    }

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
