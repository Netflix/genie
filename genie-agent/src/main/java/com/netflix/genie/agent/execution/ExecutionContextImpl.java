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

import com.google.common.collect.Lists;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.context.annotation.Lazy;
import org.springframework.statemachine.action.Action;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import java.io.File;
import java.util.Collections;
import java.util.List;
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

    private final AtomicReference<File> jobDirectoryRef = new AtomicReference<>();
    private final AtomicReference<JobSpecification> jobSpecRef = new AtomicReference<>();
    private final AtomicReference<Map<String, String>> jobEnvironmentRef = new AtomicReference<>();
    private final AtomicReference<JobStatus> finalJobStatusRef = new AtomicReference<>();
    private final AtomicReference<JobStatus> currentJobStatusRef = new AtomicReference<>();
    private final AtomicReference<String> claimedJobIdRef = new AtomicReference<>();
    private final List<StateAction> cleanupActions = Lists.newArrayList();
    private final List<Triple<States, Class<? extends Action>, Exception>> stateActionErrors = Lists.newArrayList();

    ExecutionContextImpl() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobDirectory(final File jobDirectory) {
        setIfNullOrThrow(jobDirectory, jobDirectoryRef);
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
        setIfNullOrThrow(jobSpecification, jobSpecRef);
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
    public void setJobEnvironment(final Map<String, String> jobEnvironment) {
        setIfNullOrThrow(jobEnvironment, jobEnvironmentRef);
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
    public void addCleanupActions(final StateAction stateAction) {
        synchronized (cleanupActions) {
            cleanupActions.add(stateAction);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StateAction> getCleanupActions() {
        synchronized (cleanupActions) {
            return Collections.unmodifiableList(cleanupActions);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addStateActionError(
        final States state,
        final Class<? extends Action> actionClass,
        final Exception exception
    ) {
        synchronized (stateActionErrors) {
            stateActionErrors.add(Triple.of(state, actionClass, exception));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasStateActionError() {
        synchronized (stateActionErrors) {
            return !stateActionErrors.isEmpty();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Triple<States, Class<? extends Action>, Exception>> getStateActionErrors() {
        synchronized (stateActionErrors) {
            return Collections.unmodifiableList(stateActionErrors);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFinalJobStatus(final JobStatus jobStatus) {
        if (jobStatus.isActive()) {
            throw new IllegalArgumentException("Invalid final job status: " + jobStatus);
        }
        setIfNullOrThrow(jobStatus, finalJobStatusRef);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getFinalJobStatus() {
        return finalJobStatusRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCurrentJobStatus(final JobStatus jobStatus) {
        currentJobStatusRef.set(jobStatus);
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public JobStatus getCurrentJobStatus() {
        return currentJobStatusRef.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClaimedJobId(@NotBlank final String jobId) {
        setIfNullOrThrow(jobId, claimedJobIdRef);
    }

    /**
     * {@inheritDoc}
     */
    @Nullable
    @Override
    public String getClaimedJobId() {
        return claimedJobIdRef.get();
    }

    private static <T> void setIfNullOrThrow(final T value, final AtomicReference<T> reference) {
        if (!reference.compareAndSet(null, value)) {
            throw new RuntimeException("Trying to update context object that already has a value");
        }
    }
}
