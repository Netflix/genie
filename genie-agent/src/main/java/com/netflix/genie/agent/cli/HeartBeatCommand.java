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
import com.beust.jcommander.ParametersDelegate;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Command to establish a persistent connection with Genie server and exchange heartbeats.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class HeartBeatCommand implements AgentCommand {

    private final HeartBeatCommandArguments heartBeatCommandArguments;
    private final AgentHeartBeatService agentHeartBeatService;

    private AtomicBoolean isConnected = new AtomicBoolean(false);

    HeartBeatCommand(
        final HeartBeatCommandArguments heartBeatCommandArguments,
        final AgentHeartBeatService agentHeartBeatService
    ) {
        this.heartBeatCommandArguments = heartBeatCommandArguments;
        this.agentHeartBeatService = agentHeartBeatService;
    }

    @Override
    public void run() {

        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.initialize();
        scheduler.scheduleAtFixedRate(this::checkConnectionStatus, 1000);

        final String pseudoJobId = this.getClass().getSimpleName() + UUID.randomUUID().toString();

        System.out.println("Initiating protocol with pseudo jobId: " + pseudoJobId);

        agentHeartBeatService.start(pseudoJobId);

        synchronized (this) {
            try {
                wait(TimeUnit.MILLISECONDS.convert(heartBeatCommandArguments.getRunDuration(), TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted");
            }
        }

        agentHeartBeatService.stop();
    }

    private void checkConnectionStatus() {
        final boolean isCurrentlyConnected = agentHeartBeatService.isConnected();

        final boolean wasPreviouslyConnected = isConnected.getAndSet(isCurrentlyConnected);

        if (wasPreviouslyConnected != isCurrentlyConnected) {
            System.out.println("Connection status: " + (isCurrentlyConnected ? "CONNECTED" : "DISCONNECTED"));
        }
    }

    @Component
    @Parameters(commandNames = CommandNames.HEARTBEAT, commandDescription = "Send heartbeats to a server")
    @Getter
    static class HeartBeatCommandArguments implements AgentCommandArguments {

        @ParametersDelegate
        private final ArgumentDelegates.ServerArguments serverArguments;

        @Parameter(
            names = {"--duration"},
            description = "How long to run before terminating (in seconds, 0 for unlimited)"
        )
        private int runDuration;

        HeartBeatCommandArguments(final ArgumentDelegates.ServerArguments serverArguments) {
            this.serverArguments = serverArguments;
        }


        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return HeartBeatCommand.class;
        }
    }
}
