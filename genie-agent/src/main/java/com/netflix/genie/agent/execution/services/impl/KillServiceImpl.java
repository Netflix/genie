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
package com.netflix.genie.agent.execution.services.impl;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.agent.cli.ExitCode;
import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.properties.AgentProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link KillService}.
 *
 * @author standon
 * @since 4.0.0
 */
@Slf4j
class KillServiceImpl implements KillService {

    private final ExecutionContext executionContext;
    private final AgentProperties agentProperties;
    private final ThreadFactory threadFactory;
    private final AtomicBoolean killed = new AtomicBoolean(false);

    KillServiceImpl(final ExecutionContext executionContext, final AgentProperties agentProperties) {
        this(
            executionContext,
            agentProperties,
            Thread::new
        );
    }

    @VisibleForTesting
    KillServiceImpl(
        final ExecutionContext executionContext,
        final AgentProperties agentProperties,
        final ThreadFactory threadFactory
    ) {
        this.executionContext = executionContext;
        this.agentProperties = agentProperties;
        this.threadFactory = threadFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill(final KillSource killSource) {
        if (this.killed.compareAndSet(false, true)) {
            ConsoleLog.getLogger().info("Job kill requested (source: {})", killSource.name());
            this.executionContext.getStateMachine().kill(killSource);
            this.threadFactory.newThread(this::emergencyStop).start();
        }
    }

    /**
     * This task is launched when a call to kill() is received.
     * It's extra insurance that the JVM eventually will shut down.
     * Hopefully, execution terminates due to regular shutdown procedures before this mechanism kicks-in.
     */
    @SuppressFBWarnings("DM_EXIT") // For calling System.exit
    private void emergencyStop() {
        try {
            Thread.sleep(this.agentProperties.getEmergencyShutdownDelay().toMillis());
        } catch (InterruptedException e) {
            log.warn("Emergency shutdown thread interrupted");
        }
        System.exit(ExitCode.EXEC_ABORTED.getCode());
    }
}
