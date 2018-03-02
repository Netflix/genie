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

import com.google.common.collect.Queues;
import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.common.dto.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import java.io.File;
import java.util.Deque;
import java.util.Map;
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
    private final AtomicReference<Process> jobProcessRef = new AtomicReference<>();
    private final AtomicReference<File> jobDirectoryRef = new AtomicReference<>();
    private final AtomicReference<JobSpecification> jobSpecRef = new AtomicReference<>();
    private final AtomicReference<Map<String, String>> jobEnvironmentRef = new AtomicReference<>();
    private final Deque<StateAction> cleanupActions = Queues.newArrayDeque();

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAgentId(@NotBlank final String agentId) {
        setIfNullOrTrow(agentId, agentIdRef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAgentId() {
        return agentIdRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobProcess(final Process jobProcess) {
        setIfNullOrTrow(jobProcess, jobProcessRef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Process getJobProcess() {
        return jobProcessRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobDirectory(final File jobDirectory) {
        setIfNullOrTrow(jobDirectory, jobDirectoryRef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File getJobDirectory() {
        return jobDirectoryRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobSpecification(final JobSpecification jobSpecification) {
        setIfNullOrTrow(jobSpecification, jobSpecRef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification getJobSpecification() {
        return jobSpecRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getJobEnvironment() {
        return jobEnvironmentRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobEnvironment(final Map<String, String> jobEnvironment) {
        setIfNullOrTrow(jobEnvironment, jobEnvironmentRef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Deque<StateAction> getCleanupActions() {
        return cleanupActions;
    }

    private static <T> void setIfNullOrTrow(final T value, final AtomicReference<T> reference) {
        if (!reference.compareAndSet(null, value)) {
            throw new RuntimeException("Trying to update context object that already has a value");
        }
    }
}
