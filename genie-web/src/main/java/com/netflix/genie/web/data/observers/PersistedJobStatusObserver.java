/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.data.observers;


import com.netflix.genie.common.external.dtos.v4.JobStatus;

import javax.annotation.Nullable;

/**
 * Interface for an observer that gets notified of job 'status' change after the latter is persisted.
 * This observer is invoked as callback during data/persistence methods.
 * It should NOT spend significant time processing.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface PersistedJobStatusObserver {

    /**
     * Handle a notification of job status change after the latter was successfully committed to persistent storage.
     *
     * @param jobId          the job unique id
     * @param previousStatus the previous job status, or null if this job was just created and persisted
     * @param currentStatus  the job status that was just persisted. Guaranteed to be different than the previous.
     */
    void notify(
        String jobId,
        @Nullable JobStatus previousStatus,
        JobStatus currentStatus
    );

}
