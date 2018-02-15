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

package com.netflix.genie.agent.execution;

import org.hibernate.validator.constraints.NotBlank;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Stateful context used by execution components to track state.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ThreadSafe
public interface ExecutionContext {

    /**
     * Set the unique agent identifier obtained by the server.
     *
     * @param agentId a non-blank string
     * @throws RuntimeException if the agent id is already set
     */
    void setAgentId(@NotBlank final String agentId);

    /**
     * Get the agent identifier.
     *
     * @return a non-blank string if the agent identifier was set, or null.
     */
    String getAgentId();
}
