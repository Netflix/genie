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

import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.proto.PingServiceGrpc;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import javax.validation.Validator;

/**
 * Spring auto configuration class to contain all beans involved in the CLI for the Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class CliAutoConfiguration {

    /**
     * Provide a bean for proto conversion of job service objects if one isn't already defined.
     *
     * @return A {@link JobServiceProtoConverter} instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobServiceProtoConverter.class)
    public JobServiceProtoConverter jobServiceProtoConverter() {
        return new JobServiceProtoConverter();
    }

    /**
     * Provide a bean for cache command line arguments if none was created already.
     *
     * @return a {@link CacheArgumentsImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ArgumentDelegates.CacheArguments.class)
    public ArgumentDelegates.CacheArguments cacheArgumentsDelegate() {
        return new CacheArgumentsImpl();
    }

    /**
     * Provide a bean for arguments for a download command.
     *
     * @param cacheArguments Any arguments that were provided for the cache of this agent instance
     * @return An instance of {@link com.netflix.genie.agent.cli.DownloadCommand.DownloadCommandArguments}
     */
    @Bean
    @ConditionalOnMissingBean(DownloadCommand.DownloadCommandArguments.class)
    public DownloadCommand.DownloadCommandArguments downloadCommandArguments(
        final ArgumentDelegates.CacheArguments cacheArguments
    ) {
        return new DownloadCommand.DownloadCommandArguments(cacheArguments);
    }

    /**
     * Provide a lazy bean definition for a {@link DownloadCommand} if one wasn't provided.
     *
     * @param downloadCommandArguments The download command arguments to use
     * @param downloadService          The download service to use
     * @return An instance of {@link DownloadCommand}
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(DownloadCommand.class)
    public DownloadCommand downloadCommand(
        final DownloadCommand.DownloadCommandArguments downloadCommandArguments,
        final DownloadService downloadService
    ) {
        return new DownloadCommand(downloadCommandArguments, downloadService);
    }

    /**
     * Provide a bean for execution command arguments if one hasn't already been provided.
     *
     * @param serverArguments     The server arguments to use
     * @param cacheArguments      The cache arguments to use
     * @param jobRequestArguments The job request arguments to use
     * @param cleanupArguments    The cleanup arguments to use
     * @return An instance of {@link com.netflix.genie.agent.cli.ExecCommand.ExecCommandArguments}
     */
    @Bean
    @ConditionalOnMissingBean(ExecCommand.ExecCommandArguments.class)
    public ExecCommand.ExecCommandArguments execCommandArguments(
        final ArgumentDelegates.ServerArguments serverArguments,
        final ArgumentDelegates.CacheArguments cacheArguments,
        final ArgumentDelegates.JobRequestArguments jobRequestArguments,
        final ArgumentDelegates.CleanupArguments cleanupArguments
    ) {
        return new ExecCommand.ExecCommandArguments(
            serverArguments,
            cacheArguments,
            jobRequestArguments,
            cleanupArguments
        );
    }

    /**
     * Provide a lazy bean definition for an {@link ExecCommand}.
     *
     * @param execCommandArguments     The exec command arguments to use
     * @param jobExecutionStateMachine The job execution state machine instance to use
     * @param executionContext         The execution context to use
     * @param killService              The kill service to use
     * @return A bean definition for an {@link ExecCommand} if one hasn't already been defined
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ExecCommand.class)
    public ExecCommand execCommand(
        final ExecCommand.ExecCommandArguments execCommandArguments,
        final JobExecutionStateMachine jobExecutionStateMachine,
        final ExecutionContext executionContext,
        final KillService killService
    ) {
        return new ExecCommand(execCommandArguments, jobExecutionStateMachine, executionContext, killService);
    }

    /**
     * The main {@link GenieAgentRunner} entry point bean which implements
     * {@link org.springframework.boot.CommandLineRunner}.
     *
     * @param argumentParser The argument parser to use
     * @param commandFactory The command factory to use
     * @return An instance of {@link GenieAgentRunner} if one hasn't already been provided
     */
    @Bean
    @ConditionalOnMissingBean(GenieAgentRunner.class)
    public GenieAgentRunner genieAgentRunner(final ArgumentParser argumentParser, final CommandFactory commandFactory) {
        return new GenieAgentRunner(argumentParser, commandFactory);
    }

    /**
     * Provide a bean for {@link com.netflix.genie.agent.cli.HeartBeatCommand.HeartBeatCommandArguments} if none has
     * been provided.
     *
     * @param serverArguments The server arguments to use
     * @return An instance of {@link com.netflix.genie.agent.cli.HeartBeatCommand.HeartBeatCommandArguments}
     */
    @Bean
    @ConditionalOnMissingBean(HeartBeatCommand.HeartBeatCommandArguments.class)
    public HeartBeatCommand.HeartBeatCommandArguments heartBeatCommandArguments(
        final ArgumentDelegates.ServerArguments serverArguments
    ) {
        return new HeartBeatCommand.HeartBeatCommandArguments(serverArguments);
    }

    /**
     * Provide a lazy bean definition for a {@link HeartBeatCommand} if none has been provided already.
     *
     * @param heartBeatCommandArguments The heart beat command arguments to use
     * @param agentHeartBeatService     The heart beat service to use
     * @return An instance of {@link HeartBeatCommand}
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(HeartBeatCommand.class)
    public HeartBeatCommand heartBeatCommand(
        final HeartBeatCommand.HeartBeatCommandArguments heartBeatCommandArguments,
        final AgentHeartBeatService agentHeartBeatService
    ) {
        return new HeartBeatCommand(heartBeatCommandArguments, agentHeartBeatService);
    }

    /**
     * Provide an bean of {@link com.netflix.genie.agent.cli.HelpCommand.HelpCommandArguments} if none already
     * exists.
     *
     * @return An instance of {@link com.netflix.genie.agent.cli.HelpCommand.HelpCommandArguments}
     */
    @Bean
    @ConditionalOnMissingBean(HelpCommand.HelpCommandArguments.class)
    public HelpCommand.HelpCommandArguments helpCommandArguments() {
        return new HelpCommand.HelpCommandArguments();
    }

    /**
     * Provide a lazy bean instance of {@link HelpCommand} if none has been provided already.
     *
     * @param argumentParser The argument parser to use
     * @return The {@link HelpCommand} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(HelpCommand.class)
    public HelpCommand helpCommand(final ArgumentParser argumentParser) {
        return new HelpCommand(argumentParser);
    }

    /**
     * Provide an instance of {@link com.netflix.genie.agent.cli.InfoCommand.InfoCommandArguments} if none already
     * exists.
     *
     * @return An {@link com.netflix.genie.agent.cli.InfoCommand.InfoCommandArguments} instance
     */
    @Bean
    @ConditionalOnMissingBean(InfoCommand.InfoCommandArguments.class)
    public InfoCommand.InfoCommandArguments infoCommandArguments() {
        return new InfoCommand.InfoCommandArguments();
    }

    /**
     * Provide a lazy bean definition for an {@link InfoCommand} if none already exists.
     *
     * @param infoCommandArguments           The info command arguments to use
     * @param configurableApplicationContext The Spring context to use
     * @param agentMetadata                  The agent metadata to use
     * @return A lazy bean definition for an {@link InfoCommand} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(InfoCommand.class)
    public InfoCommand infoCommand(
        final InfoCommand.InfoCommandArguments infoCommandArguments,
        final ConfigurableApplicationContext configurableApplicationContext,
        final AgentMetadata agentMetadata
    ) {
        return new InfoCommand(infoCommandArguments, configurableApplicationContext, agentMetadata);
    }

    /**
     * Provide a {@link com.netflix.genie.agent.cli.ArgumentDelegates.JobRequestArguments} instance if none has already
     * been provided.
     *
     * @return An instance of {@link com.netflix.genie.agent.cli.ArgumentDelegates.JobRequestArguments}
     */
    @Bean
    @ConditionalOnMissingBean(ArgumentDelegates.JobRequestArguments.class)
    public ArgumentDelegates.JobRequestArguments jobRequestArguments() {
        return new JobRequestArgumentsImpl();
    }

    /**
     * Provide an instance of {@link JobRequestConverter} if none has already been provided.
     *
     * @param validator The validator to use
     * @return A {@link JobRequestConverter} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(JobRequestConverter.class)
    public JobRequestConverter jobRequestConverter(final Validator validator) {
        return new JobRequestConverter(validator);
    }

    /**
     * Provides a {@link com.netflix.genie.agent.cli.PingCommand.PingCommandArguments} instance if none has already been
     * provided.
     *
     * @param serverArguments The server arguments to use
     * @return A {@link com.netflix.genie.agent.cli.PingCommand.PingCommandArguments} instance
     */
    @Bean
    @ConditionalOnMissingBean(PingCommand.PingCommandArguments.class)
    public PingCommand.PingCommandArguments pingCommandArguments(
        final ArgumentDelegates.ServerArguments serverArguments
    ) {
        return new PingCommand.PingCommandArguments(serverArguments);
    }

    /**
     * Provide a lazy bean for a {@link PingCommand} if none has already been provided.
     *
     * @param pingCommandArguments  The ping command arguments to use
     * @param pingServiceFutureStub The gRPC future stub to use
     * @param agentMetadata         The agent metadata to use
     * @return A {@link PingCommand} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(PingCommand.class)
    public PingCommand pingCommand(
        final PingCommand.PingCommandArguments pingCommandArguments,
        final PingServiceGrpc.PingServiceFutureStub pingServiceFutureStub,
        final AgentMetadata agentMetadata
    ) {
        return new PingCommand(pingCommandArguments, pingServiceFutureStub, agentMetadata);
    }

    /**
     * Provide a bean of type {@link com.netflix.genie.agent.cli.ResolveJobSpecCommand.ResolveJobSpecCommandArguments}
     * if none has already been provided.
     *
     * @param serverArguments     The server arguments to use
     * @param jobRequestArguments The job request arguments to use
     * @return An instance of {@link com.netflix.genie.agent.cli.ResolveJobSpecCommand.ResolveJobSpecCommandArguments}
     */
    @Bean
    @ConditionalOnMissingBean(ResolveJobSpecCommand.ResolveJobSpecCommandArguments.class)
    public ResolveJobSpecCommand.ResolveJobSpecCommandArguments resolveJobSpecCommandArguments(
        final ArgumentDelegates.ServerArguments serverArguments,
        final ArgumentDelegates.JobRequestArguments jobRequestArguments
    ) {
        return new ResolveJobSpecCommand.ResolveJobSpecCommandArguments(serverArguments, jobRequestArguments);
    }

    /**
     * Provide a lazy bean definition for a {@link ResolveJobSpecCommand} if none has already been provided.
     *
     * @param resolveJobSpecCommandArguments The resolve job spec arguments to use
     * @param agentJobService                The agent job service to use
     * @param jobRequestConverter            The job request converter to use
     * @return A {@link ResolveJobSpecCommand} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(ResolveJobSpecCommand.class)
    public ResolveJobSpecCommand resolveJobSpecCommand(
        final ResolveJobSpecCommand.ResolveJobSpecCommandArguments resolveJobSpecCommandArguments,
        final AgentJobService agentJobService,
        final JobRequestConverter jobRequestConverter
    ) {
        return new ResolveJobSpecCommand(resolveJobSpecCommandArguments, agentJobService, jobRequestConverter);
    }

    /**
     * Provide a {@link com.netflix.genie.agent.cli.ArgumentDelegates.ServerArguments} bean if none has already been
     * defined.
     *
     * @return A {@link ServerArgumentsImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ArgumentDelegates.ServerArguments.class)
    public ArgumentDelegates.ServerArguments serverArguments() {
        return new ServerArgumentsImpl();
    }

    /**
     * Provide a {@link com.netflix.genie.agent.cli.ArgumentDelegates.CleanupArguments} bean if none has already been
     * defined.
     *
     * @return A {@link CleanupArgumentsImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ArgumentDelegates.CleanupArguments.class)
    public ArgumentDelegates.CleanupArguments cleanupArguments() {
        return new CleanupArgumentsImpl();
    }
}
