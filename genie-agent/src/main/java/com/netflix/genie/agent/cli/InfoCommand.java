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
import com.google.common.collect.Lists;
import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.PropertySources;

import javax.validation.constraints.Min;
import java.util.List;
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

    private static final String NEWLINE = System.lineSeparator();
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
            .append(NEWLINE)
            .append("  version: ")
            .append(agentMetadata.getAgentVersion())
            .append(NEWLINE)
            .append("  host: ")
            .append(agentMetadata.getAgentHostName())
            .append(NEWLINE)
            .append("  pid: ")
            .append(agentMetadata.getAgentPid())
            .append(NEWLINE);

        messageBuilder
            .append("Active profiles:")
            .append(NEWLINE);

        for (String profileName : applicationContext.getEnvironment().getActiveProfiles()) {
            messageBuilder
                .append("  - ")
                .append(profileName)
                .append(NEWLINE);
        }

        messageBuilder
            .append("Default profiles:")
            .append(NEWLINE);

        for (String profileName : applicationContext.getEnvironment().getDefaultProfiles()) {
            messageBuilder
                .append("  - ")
                .append(profileName)
                .append(NEWLINE);
        }

        if (infoCommandArguments.getIncludeBeans()) {
            messageBuilder
                .append("Beans in context: ")
                .append(applicationContext.getBeanDefinitionCount())
                .append(NEWLINE);

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
                    .append(NEWLINE);
            }
        }

        if (infoCommandArguments.getIncludeEnvironment()) {

            final Set<Map.Entry<String, Object>> envEntries =
                applicationContext.getEnvironment().getSystemEnvironment().entrySet();

            messageBuilder
                .append("Environment variables: ")
                .append(envEntries.size())
                .append(NEWLINE);

            for (Map.Entry<String, Object> envEntry : envEntries) {
                messageBuilder
                    .append(
                        String.format(
                            "  - %s=%s",
                            envEntry.getKey(),
                            envEntry.getValue()
                        )
                    )
                    .append(NEWLINE);
            }
        }

        if (infoCommandArguments.getIncludeProperties()) {

            final Set<Map.Entry<String, Object>> properties =
                applicationContext.getEnvironment().getSystemProperties().entrySet();

            messageBuilder
                .append("Properties: ")
                .append(properties.size())
                .append(NEWLINE);
            for (Map.Entry<String, Object> property : properties) {
                messageBuilder
                    .append(
                        String.format(
                            "  - %s=%s",
                            property.getKey(),
                            property.getValue()
                        )
                    )
                    .append(NEWLINE);
            }

            final PropertySources propertySources = applicationContext.getEnvironment().getPropertySources();
            messageBuilder
                .append("Property sources: ")
                .append(NEWLINE);
            for (PropertySource<?> propertySource : propertySources) {
                messageBuilder
                    .append(
                        String.format(
                            "  - %s (%s)",
                            propertySource.getName(),
                            propertySource.getClass().getSimpleName()
                        )
                    )
                    .append(NEWLINE);
            }
        }

        if (infoCommandArguments.getIncludeStateMachine()) {
            messageBuilder
                .append("Job execution state machine: ")
                .append(NEWLINE)
                .append("// ------------------------------------------------------------------------")
                .append(NEWLINE);

            final JobExecutionStateMachine jobExecutionStateMachine
                = applicationContext.getBean(JobExecutionStateMachine.class);

            final List<ExecutionStage> stages = jobExecutionStateMachine.getExecutionStages();

            createStateMachineDotGraph(stages, messageBuilder);
        }

        System.out.println(messageBuilder);

        return ExitCode.SUCCESS;
    }

    private void createStateMachineDotGraph(
        final List<ExecutionStage> stages,
        final StringBuilder messageBuilder
    ) {
        final List<States> states = Lists.newArrayList();

        states.add(States.INITIAL_STATE);
        stages.forEach(stage -> states.add(stage.getState()));
        states.add(States.FINAL_STATE);

        final List<Pair<States, States>> transitions = Lists.newArrayList();

        for (int i = 0; i < states.size() - 1; i++) {
            transitions.add(Pair.of(states.get(i), states.get(i + 1)));
        }

        messageBuilder
            .append("digraph state_machine {")
            .append(NEWLINE);

        messageBuilder
            .append("  // States")
            .append(NEWLINE);


        states.forEach(
            state -> {
                final String shape;
                if (state == States.INITIAL_STATE) {
                    shape = "shape=diamond";
                } else if (state == States.FINAL_STATE) {
                    shape = "shape=rectangle";
                } else {
                    shape = "";
                }

                final String edgeTickness;
                if (state.isCriticalState()) {
                    edgeTickness = "penwidth=3.0";
                } else {
                    edgeTickness = "";
                }

                messageBuilder
                    .append("  ")
                    .append(state.name())
                    .append(" [label=")
                    .append(state.name())
                    .append(" ")
                    .append(shape)
                    .append(" ")
                    .append(edgeTickness)
                    .append("]")
                    .append(NEWLINE);
            }
        );

        messageBuilder
            .append("  // Transitions")
            .append(NEWLINE);

        transitions.forEach(
            transition ->
                messageBuilder
                    .append("  ")
                    .append(transition.getLeft())
                    .append(" -> ")
                    .append(transition.getRight())
                    .append(NEWLINE)
        );

        messageBuilder
            .append("  // Skip transitions")
            .append(NEWLINE);

        // Draw an edge from a state whose next state is skippable to the next non-skippable state.
        for (int i = 0; i < states.size() - 2; i++) {
            final States state = states.get(i);
            final boolean skipNext = states.get(i + 1).isSkippedDuringAbortedExecution();
            if (skipNext) {
                // Find the next non-skipped stage
                for (int j = i + 1; j < states.size() - 1; j++) {
                    final States nextState = states.get(j);
                    if (!nextState.isSkippedDuringAbortedExecution()) {
                        messageBuilder
                            .append("  ")
                            .append(state)
                            .append(" -> ")
                            .append(nextState)
                            .append(" [style=dotted]")
                            .append(NEWLINE);
                        break;
                    }
                }
            }
        }

        messageBuilder
            .append("  // Retry transitions")
            .append(NEWLINE);

        states.forEach(
            state -> {
                final @Min(0) int retries = state.getTransitionRetries();
                if (retries > 0) {
                    messageBuilder
                        .append("  ")
                        .append(state)
                        .append(" -> ")
                        .append(state)
                        .append(" [style=dashed label=\"")
                        .append(retries)
                        .append(" retries\"]")
                        .append(NEWLINE);
                }
            }
        );

        messageBuilder
            .append("}")
            .append(NEWLINE)
            .append("// ------------------------------------------------------------------------")
            .append(NEWLINE);
    }

    @Parameters(commandNames = CommandNames.INFO, commandDescription = "Print agent and environment information")
    @Getter
    static class InfoCommandArguments implements AgentCommandArguments {
        @Parameter(names = {"--beans"}, description = "Print beans")
        private Boolean includeBeans = true;
        @Parameter(names = {"--env"}, description = "Print environment variables")
        private Boolean includeEnvironment = true;
        @Parameter(names = {"--properties"}, description = "Print properties")
        private Boolean includeProperties = true;
        @Parameter(names = {"--state-machine"}, description = "Print job execution state machine in (.dot notation)")
        private Boolean includeStateMachine = false;

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return InfoCommand.class;
        }
    }
}
