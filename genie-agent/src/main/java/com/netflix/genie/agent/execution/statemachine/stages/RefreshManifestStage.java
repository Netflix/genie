/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine.stages;

import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Triggers a manual refresh of the cached files manifest.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class RefreshManifestStage extends ExecutionStage {
    private final AgentFileStreamService agentFileStreamService;

    /**
     * Constructor.
     *
     * @param agentFileStreamService agent file stream service
     * @param state                  the associated state
     */
    public RefreshManifestStage(final AgentFileStreamService agentFileStreamService, final States state) {
        super(state);
        this.agentFileStreamService = agentFileStreamService;
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {
        if (executionContext.getJobDirectory() != null) {
            log.info("Forcing a manifest refresh");
            final Optional<ScheduledFuture<?>> optionalFuture = this.agentFileStreamService.forceServerSync();
            if (optionalFuture.isPresent()) {
                // Give it a little time to complete, but don't block execution if it doesn't
                final Duration timeout = executionContext.getAgentProperties().getForceManifestRefreshTimeout();
                try {
                    optionalFuture.get().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException | TimeoutException e) {
                    log.warn("Forced manifest refresh timeout: {}", e.getMessage());
                } catch (ExecutionException e) {
                    log.error("Failed to force push manifest", e);
                }
            }
        }
    }
}
