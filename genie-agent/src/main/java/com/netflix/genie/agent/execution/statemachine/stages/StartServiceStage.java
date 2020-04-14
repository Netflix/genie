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

import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;

/**
 * Base class for stages starting a service.
 */
@Slf4j
abstract class StartServiceStage extends ExecutionStage {

    /**
     * Constructor.
     *
     * @param state the associated state
     */
    StartServiceStage(final States state) {
        super(state);
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        @NotBlank final String claimedJobId = executionContext.getClaimedJobId();
        assert StringUtils.isNotBlank(claimedJobId);

        try {
            log.info("Starting service in stage {}", this.getClass().getSimpleName());
            this.startService(claimedJobId, executionContext);
        } catch (RuntimeException e) {
            throw createFatalException(e);
        }
    }

    protected abstract void startService(@NotBlank String claimedJobId, ExecutionContext executionContext);
}
