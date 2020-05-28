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

import com.netflix.genie.agent.cli.ExitCode;
import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.properties.AgentProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;

/**
 * Implementation of {@link KillService}.
 *
 * @author standon
 * @since 4.0.0
 */
@Slf4j
class KillServiceImpl implements KillService {

    private static final Runnable DEFAULT_ACTION = () -> System.exit(ExitCode.EXEC_ABORTED.getCode());
    private static final String EMERGENCY_TERMINATION_THREAD_NAME = "emergency-shutdown";

    private final ApplicationEventPublisher applicationEventPublisher;
    private final Thread emergencyTerminationThread;
    private final AgentProperties agentProperties;

    KillServiceImpl(final ApplicationEventPublisher applicationEventPublisher, final AgentProperties agentProperties) {
        this(
            applicationEventPublisher,
            agentProperties, DEFAULT_ACTION
        );
    }

    KillServiceImpl(
        final ApplicationEventPublisher applicationEventPublisher,
        final AgentProperties agentProperties,
        final Runnable emergencyTerminationAction
    ) {
        this.applicationEventPublisher = applicationEventPublisher;
        this.agentProperties = agentProperties;
        this.emergencyTerminationThread = new Thread(
            () -> {
                log.debug("Emergency shutdown countdown started");
                try {
                    Thread.sleep(this.agentProperties.getEmergencyShutdownDelay().toMillis());
                } catch (InterruptedException e) {
                    log.warn("Interrupted during delayed emergency countdown");
                }
                log.warn("Emergency shutdown now");
                emergencyTerminationAction.run();
            },
            EMERGENCY_TERMINATION_THREAD_NAME
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill(final KillSource killSource) {
        ConsoleLog.getLogger().info("Job kill requested (source: {})", killSource.name());
        log.debug("Publishing kill event");
        this.applicationEventPublisher.publishEvent(new KillEvent(killSource));

        // Start emergency termination thread, if not already running
        if (!this.emergencyTerminationThread.isAlive()) {
            this.emergencyTerminationThread.start();
        }
    }
}
