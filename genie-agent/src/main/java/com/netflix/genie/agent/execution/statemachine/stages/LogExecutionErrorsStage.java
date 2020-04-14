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
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Logs execution errors encountered so far.
 * This stage is positioned before the archive step to ensure the archived log file is useful for post-execution
 * debugging.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class LogExecutionErrorsStage extends com.netflix.genie.agent.execution.statemachine.ExecutionStage {
    /**
     * Constructor.
     */
    public LogExecutionErrorsStage() {
        super(States.LOG_EXECUTION_ERRORS);
    }

    @Override
    protected void attemptStageAction(
        final ExecutionContext executionContext
    ) throws RetryableJobExecutionException, FatalJobExecutionException {

        final List<ExecutionContext.TransitionExceptionRecord> records;
        records = executionContext.getTransitionExceptionRecords();

        if (records.isEmpty()) {
            log.info("No transition errors recorded");
        } else {
            log.info("Transition errors recorded so far:");
            records.forEach(
                record -> {
                    final Exception exception = record.getRecordedException();
                    final States state = record.getState();

                    log.info(
                        " * {} error in state {}: {} - {}",
                        exception instanceof FatalJobExecutionException ? "Fatal" : "Retryable",
                        state.name(),
                        exception.getClass().getSimpleName(),
                        exception.getMessage()
                    );
                    Throwable cause = exception.getCause();
                    while (cause != null) {
                        log.info(
                            "   > Cause: {} - {}",
                            cause.getClass().getSimpleName(),
                            cause.getMessage()
                        );
                        cause = cause.getCause();
                    }
                }
            );
        }
    }
}
