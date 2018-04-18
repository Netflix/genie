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
package com.netflix.genie.web.services;

import com.netflix.genie.common.dto.v4.AgentEvent;
import org.springframework.validation.annotation.Validated;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;

/**
 * Service API definition for handling Agent events.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ParametersAreNonnullByDefault
@Validated
public interface AgentEventsService {

    /**
     * Handle incoming agent event.
     * @param jobStatusUpdate the event
     */
    void handleAgentEvent(@Valid final AgentEvent.JobStatusUpdate jobStatusUpdate);

    /**
     * Handle incoming agent event.
     * @param stateChange the event
     */
    void handleAgentEvent(@Valid final AgentEvent.StateChange stateChange);

    /**
     * Handle incoming agent event.
     * @param stateActionExecution the event
     */
    void handleAgentEvent(@Valid final AgentEvent.StateActionExecution stateActionExecution);
}
