/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine.listeners;

import brave.Tracer;
import com.netflix.genie.agent.execution.statemachine.States;

/**
 * A listener which adds data to spans based on the events emitted by the state machine.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class TracingListener implements JobExecutionListener {

    static final String ENTERED_STATE_ANNOTATION_PREFIX = "Entered execution state: ";
    static final String EXITED_STATE_ANNOTATION_PREFIX = "Exited execution state: ";

    private final Tracer tracer;

    /**
     * Constructor.
     *
     * @param tracer The {@link Tracer} instance to use for instrumentation
     */
    public TracingListener(final Tracer tracer) {
        this.tracer = tracer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateEntered(final States state) {
        this.tracer.currentSpanCustomizer().annotate(ENTERED_STATE_ANNOTATION_PREFIX + state);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateExited(final States state) {
        this.tracer.currentSpanCustomizer().annotate(EXITED_STATE_ANNOTATION_PREFIX + state);
    }
}
