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
package com.netflix.genie.agent.execution.statemachine;

import lombok.Getter;

import javax.annotation.Nullable;

/**
 * Fatal exception that should stop execution early. For example, claiming a job that is not in claimable state.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
public class FatalJobExecutionException extends RuntimeException {

    private final States sourceState;

    /**
     * Constructor without cause.
     *
     * @param sourceState state in which error was encountered
     * @param message     message
     */
    public FatalJobExecutionException(
        final States sourceState,
        final String message
    ) {
        this(sourceState, message, null);
    }

    /**
     * Constructor.
     *
     * @param sourceState state in which error was encountered
     * @param message     message
     * @param cause       cause
     */
    public FatalJobExecutionException(
        final States sourceState,
        final String message,
        @Nullable final Throwable cause
    ) {
        super(message, cause);
        this.sourceState = sourceState;
    }
}
