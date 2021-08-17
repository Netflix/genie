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
package com.netflix.genie.agent.cli

import brave.ScopedSpan
import brave.Tracer
import brave.propagation.TraceContext
import com.beust.jcommander.ParameterException
import com.netflix.genie.common.internal.tracing.TracingConstants
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.core.env.Environment
import spock.lang.Specification

class GenieAgentRunnerSpec extends Specification {
    ArgumentParser argsParser
    AgentCommand agentCommand
    CommandFactory cmdFactory
    Environment environment
    String[] args
    GenieAgentRunner runner
    Tracer tracer
    BraveTracePropagator tracePropagator
    BraveTracingCleanup traceCleaner
    BraveTagAdapter tagAdapter
    ScopedSpan runSpan
    TraceContext initContext

    void setup() {
        this.argsParser = Mock(ArgumentParser.class)
        this.agentCommand = Mock(AgentCommand.class)
        this.cmdFactory = Mock(CommandFactory.class)
        this.environment = Mock(Environment)
        this.tracer = Mock(Tracer)
        this.tracePropagator = Mock(BraveTracePropagator)
        this.traceCleaner = Mock(BraveTracingCleanup)
        this.tagAdapter = Mock(BraveTagAdapter)
        this.args = new String[0]
        this.runSpan = Mock(ScopedSpan)
        this.runner = new GenieAgentRunner(
            this.argsParser,
            this.cmdFactory,
            new BraveTracingComponents(this.tracer, this.tracePropagator, this.traceCleaner, this.tagAdapter),
            this.environment
        )

        this.initContext = TraceContext.newBuilder()
            .traceId(UUID.randomUUID().getMostSignificantBits())
            .spanId(UUID.randomUUID().getMostSignificantBits())
            .sampled(false)
            .build()
    }

    def "Successful run"() {
        when:
        this.runner.run(this.args)

        then:
        1 * this.tracePropagator.extract(_ as Map) >> Optional.empty()
        0 * this.tracer.startScopedSpanWithParent(_ as String, _ as TraceContext)
        1 * this.tracer.startScopedSpan(GenieAgentRunner.RUN_SPAN_NAME) >> this.runSpan
        1 * this.runSpan.context() >> this.initContext
        1 * this.runSpan.finish()
        1 * this.traceCleaner.cleanup()
        1 * this.tracer.currentSpanCustomizer() >> this.runSpan
        1 * this.tagAdapter.tag(
            this.runSpan,
            TracingConstants.AGENT_CLI_COMMAND_NAME_TAG,
            TestCommands.ExampleCommand1.NAME
        )
        1 * this.argsParser.parse(this.args)
        1 * this.argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        1 * this.argsParser.getCommandNames() >> TestCommands.allCommandNames()
        1 * this.cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> this.agentCommand
        1 * this.agentCommand.run() >> ExitCode.SUCCESS

        expect:
        ExitCode.SUCCESS.getCode() == this.runner.getExitCode()
    }

    def "Argument parse error"() {
        def exception = new ParameterException("...")

        when:
        this.runner.run(this.args)

        then:
        1 * this.tracePropagator.extract(_ as Map) >> Optional.empty()
        0 * this.tracer.startScopedSpanWithParent(_ as String, _ as TraceContext)
        1 * this.tracer.startScopedSpan(GenieAgentRunner.RUN_SPAN_NAME) >> this.runSpan
        1 * this.runSpan.context() >> this.initContext
        1 * this.runSpan.error(_ as Throwable)
        1 * this.runSpan.finish()
        1 * this.traceCleaner.cleanup()
        1 * this.tracer.currentSpanCustomizer() >> this.runSpan
        0 * this.tagAdapter.tag(
            this.runSpan,
            TracingConstants.AGENT_CLI_COMMAND_NAME_TAG,
            TestCommands.ExampleCommand1.NAME
        )
        1 * this.argsParser.parse(this.args) >> { throw exception }
        0 * this.argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        0 * this.argsParser.getCommandNames() >> TestCommands.allCommandNames()
        0 * this.cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> this.agentCommand
        0 * this.agentCommand.run()

        expect:
        ExitCode.INVALID_ARGS.getCode() == this.runner.getExitCode()
    }

    def "Select no command"() {
        when:
        this.runner.run(this.args)

        then:
        1 * this.tracePropagator.extract(_ as Map) >> Optional.empty()
        0 * this.tracer.startScopedSpanWithParent(_ as String, _ as TraceContext)
        1 * this.tracer.startScopedSpan(GenieAgentRunner.RUN_SPAN_NAME) >> this.runSpan
        1 * this.runSpan.context() >> this.initContext
        1 * this.runSpan.finish()
        1 * this.runSpan.error(_ as Throwable)
        1 * this.traceCleaner.cleanup()
        1 * this.tracer.currentSpanCustomizer() >> this.runSpan
        0 * this.tagAdapter.tag(
            this.runSpan,
            TracingConstants.AGENT_CLI_COMMAND_NAME_TAG,
            TestCommands.ExampleCommand1.NAME
        )
        1 * this.argsParser.parse(this.args)
        1 * this.argsParser.getSelectedCommand() >> null
        1 * this.argsParser.getCommandNames() >> TestCommands.allCommandNames()
        0 * this.cmdFactory.get(_)
        0 * this.agentCommand.run()

        expect:
        ExitCode.INVALID_ARGS.getCode() == runner.getExitCode()
    }

    def "Select invalid command"() {
        when:
        this.runner.run(this.args)

        then:
        1 * this.tracePropagator.extract(_ as Map) >> Optional.empty()
        0 * this.tracer.startScopedSpanWithParent(_ as String, _ as TraceContext)
        1 * this.tracer.startScopedSpan(GenieAgentRunner.RUN_SPAN_NAME) >> this.runSpan
        1 * this.runSpan.context() >> this.initContext
        1 * this.runSpan.finish()
        1 * this.runSpan.error(_ as Throwable)
        1 * this.traceCleaner.cleanup()
        1 * this.tracer.currentSpanCustomizer() >> this.runSpan
        0 * this.tagAdapter.tag(
            this.runSpan,
            TracingConstants.AGENT_CLI_COMMAND_NAME_TAG,
            "foo"
        )
        1 * this.argsParser.parse(this.args)
        1 * this.argsParser.getSelectedCommand() >> "foo"
        1 * this.argsParser.getCommandNames() >> TestCommands.allCommandNames()
        0 * this.cmdFactory.get(_)
        0 * this.agentCommand.run()

        expect:
        ExitCode.INVALID_ARGS.getCode() == runner.getExitCode()
    }

    def "Fail to create command bean"() {
        when:
        this.runner.run(this.args)

        then:
        1 * this.tracePropagator.extract(_ as Map) >> Optional.empty()
        0 * this.tracer.startScopedSpanWithParent(_ as String, _ as TraceContext)
        1 * this.tracer.startScopedSpan(GenieAgentRunner.RUN_SPAN_NAME) >> this.runSpan
        1 * this.runSpan.context() >> this.initContext
        1 * this.runSpan.finish()
        1 * this.runSpan.error(_ as Throwable)
        1 * this.traceCleaner.cleanup()
        1 * this.tracer.currentSpanCustomizer() >> this.runSpan
        1 * this.tagAdapter.tag(
            this.runSpan,
            TracingConstants.AGENT_CLI_COMMAND_NAME_TAG,
            TestCommands.ExampleCommand1.NAME
        )
        1 * this.argsParser.parse(this.args)
        1 * this.argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        1 * this.argsParser.getCommandNames() >> TestCommands.allCommandNames()
        1 * this.cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> { throw new NoSuchBeanDefinitionException("...") }
        0 * this.agentCommand.run()

        expect:
        ExitCode.COMMAND_INIT_FAIL.getCode() == runner.getExitCode()
    }

    def "Execution exception"() {
        when:
        this.runner.run(this.args)

        then:
        1 * this.tracePropagator.extract(_ as Map) >> Optional.empty()
        0 * this.tracer.startScopedSpanWithParent(_ as String, _ as TraceContext)
        1 * this.tracer.startScopedSpan(GenieAgentRunner.RUN_SPAN_NAME) >> this.runSpan
        1 * this.runSpan.context() >> this.initContext
        1 * this.runSpan.finish()
        1 * this.runSpan.error(_ as Throwable)
        1 * this.traceCleaner.cleanup()
        1 * this.tracer.currentSpanCustomizer() >> this.runSpan
        1 * this.tagAdapter.tag(
            this.runSpan,
            TracingConstants.AGENT_CLI_COMMAND_NAME_TAG,
            TestCommands.ExampleCommand1.NAME
        )
        1 * argsParser.parse(args)
        1 * argsParser.getSelectedCommand() >> TestCommands.ExampleCommand1.NAME
        1 * argsParser.getCommandNames() >> TestCommands.allCommandNames()
        1 * cmdFactory.get(TestCommands.ExampleCommand1.NAME) >> this.agentCommand
        1 * agentCommand.run() >> { throw new RuntimeException("...") }

        expect:
        ExitCode.EXEC_FAIL.getCode() == runner.getExitCode()
    }
}
