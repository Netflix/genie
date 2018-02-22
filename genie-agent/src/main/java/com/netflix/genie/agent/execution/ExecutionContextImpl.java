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

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe implementation of ExecutionContext.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
@ThreadSafe
class ExecutionContextImpl implements ExecutionContext {

    private final AtomicReference<String> agentIdRef = new AtomicReference<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAgentId(@NotBlank final String agentId) {
        if (!this.agentIdRef.compareAndSet(null, agentId)) {
            throw new RuntimeException("Agent id is already set");
        }
        log.debug("Set agent id: {}", this.agentIdRef.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAgentId() {
        return agentIdRef.get();
    }
}
