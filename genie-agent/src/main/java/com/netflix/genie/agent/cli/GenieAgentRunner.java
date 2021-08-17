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

import brave.ScopedSpan;
import brave.SpanCustomizer;
import brave.Tracer;
import brave.propagation.TraceContext;
import com.beust.jcommander.ParameterException;
import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.common.internal.tracing.TracingConstants;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.core.env.Environment;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

/**
 * Main entry point for execution after the application is initialized.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GenieAgentRunner implements CommandLineRunner, ExitCodeGenerator {

    static final String RUN_SPAN_NAME = "genie-agent-run";

    private final ArgumentParser argumentParser;
    private final CommandFactory commandFactory;
    private final Environment environment;
    private final BraveTracePropagator tracePropagator;
    private final BraveTracingCleanup tracingCleanup;
    private final Tracer tracer;
    private final BraveTagAdapter tagAdapter;
    private ExitCode exitCode = ExitCode.INIT_FAIL;

    GenieAgentRunner(
        final ArgumentParser argumentParser,
        final CommandFactory commandFactory,
        final BraveTracingComponents tracingComponents,
        final Environment environment
    ) {
        this.argumentParser = argumentParser;
        this.commandFactory = commandFactory;
        this.tracePropagator = tracingComponents.getTracePropagator();
        this.tracingCleanup = tracingComponents.getTracingCleaner();
        this.tracer = tracingComponents.getTracer();
        this.tagAdapter = tracingComponents.getTagAdapter();
        this.environment = environment;
    }

    @Override
    public void run(final String... args) throws Exception {
        final ScopedSpan runSpan = this.initializeTracing();
        try {
            ConsoleLog.printBanner(this.environment);
            try {
                internalRun(args);
            } catch (final Throwable t) {
                final Throwable userConsoleException = t.getCause() != null ? t.getCause() : t;
                ConsoleLog.getLogger().error(
                    "Command execution failed: {}",
                    userConsoleException.getMessage()
                );

                log.error("Command execution failed", t);
                runSpan.error(t);
            }
        } finally {
            runSpan.finish();
            this.tracingCleanup.cleanup();
        }
    }

    private void internalRun(final String[] args) {
        final SpanCustomizer span = this.tracer.currentSpanCustomizer();
        log.info("Parsing arguments: {}", Arrays.toString(args));

        this.exitCode = ExitCode.INVALID_ARGS;

        //TODO: workaround for https://jira.spring.io/browse/SPR-17416
        final String[] originalArgs = Util.unmangleBareDoubleDash(args);
        log.debug("Arguments: {}", Arrays.toString(originalArgs));

        try {
            this.argumentParser.parse(originalArgs);
        } catch (ParameterException e) {
            throw new IllegalArgumentException("Failed to parse arguments: " + e.getMessage(), e);
        }

        final String commandName = this.argumentParser.getSelectedCommand();
        final Set<String> availableCommands = this.argumentParser.getCommandNames();
        final String availableCommandsString = Arrays.toString(availableCommands.toArray());

        if (commandName == null) {
            throw new IllegalArgumentException("No command selected -- commands available: " + availableCommandsString);
        } else if (!availableCommands.contains(commandName)) {
            throw new IllegalArgumentException("Invalid command -- commands available: " + availableCommandsString);
        }
        this.tagAdapter.tag(span, TracingConstants.AGENT_CLI_COMMAND_NAME_TAG, commandName);

        ConsoleLog.getLogger().info("Preparing agent to execute command: '{}'", commandName);

        log.info("Initializing command: {}", commandName);
        this.exitCode = ExitCode.COMMAND_INIT_FAIL;
        final AgentCommand command = this.commandFactory.get(commandName);

        this.exitCode = ExitCode.EXEC_FAIL;
        this.exitCode = command.run();
    }

    @Override
    public int getExitCode() {
        ConsoleLog.getLogger().info(
            "Terminating with code: {} ({})",
            this.exitCode.getCode(),
            this.exitCode.getMessage()
        );
        return this.exitCode.getCode();
    }

    private ScopedSpan initializeTracing() {
        // Attempt to extract any existing trace information from the environment
        final Optional<TraceContext> existingTraceContext = this.tracePropagator.extract(System.getenv());
        final ScopedSpan runSpan = existingTraceContext.isPresent()
            ? this.tracer.startScopedSpanWithParent(RUN_SPAN_NAME, existingTraceContext.get())
            : this.tracer.startScopedSpan(RUN_SPAN_NAME);
        // Quickly create and report an initial span
        final TraceContext initContext = runSpan.context();
        final String traceId = initContext.traceIdString();
        log.info("Trace ID: {}", traceId);
        ConsoleLog.getLogger().info("Trace ID: {}", traceId);
        return runSpan;
    }
}
