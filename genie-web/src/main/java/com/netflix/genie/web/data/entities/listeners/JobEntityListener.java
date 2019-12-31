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
package com.netflix.genie.web.data.entities.listeners;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.v4.EntityDtoConverters;
import com.netflix.genie.web.data.observers.PersistedJobStatusObserver;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.PostLoad;
import javax.persistence.PostUpdate;
import java.util.Optional;

/**
 * Listener for Job JPA entity ({@link JobEntity}).
 * Currently tracks persistent changes to the status of a job and notifies an observer.
 * Could be extended to do proxy more persist changes.
 * <p>
 * N.B. Spring configuration.
 * - This class does not appear in any AutoConfiguration as bean.
 * It is referenced as {@link java.util.EventListener} by {@link JobEntity}.
 * EntityManager creates it even if a bean of the same type exists already.
 * - The constructor parameter {@code persistedJobStatusObserver} is marked {@code Nullable} so that an instance can be
 * created even if no bean of that type is configured.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobEntityListener {

    private final PersistedJobStatusObserver persistedJobStatusObserver;

    /**
     * Constructor.
     *
     * @param persistedJobStatusObserver the observer to notify of persisted job status changes
     */
    public JobEntityListener(final PersistedJobStatusObserver persistedJobStatusObserver) {
        this.persistedJobStatusObserver = persistedJobStatusObserver;
    }

    /**
     * Persistence callback invoked after a job entity has been committed/flushed into persistent storage.
     * If the persisted status of the job is different from the last one notified, then a notification is emitted.
     *
     * @param jobEntity the job that was just persisted
     */
    @PostUpdate
    public void jobUpdate(final JobEntity jobEntity) {
        final JobStatus currentState;
        try {
            currentState = EntityDtoConverters.toJobStatus(jobEntity.getStatus());
        } catch (final IllegalArgumentException e) {
            log.error("Unable to convert current status {} to a valid JobStatus", jobEntity.getStatus(), e);
            return;
        }
        final JobStatus previouslyNotifiedState;
        final Optional<String> pnsOptional = jobEntity.getNotifiedJobStatus();
        if (pnsOptional.isPresent()) {
            try {
                previouslyNotifiedState = EntityDtoConverters.toJobStatus(pnsOptional.get());
            } catch (final IllegalArgumentException e) {
                log.error(
                    "Unable to convert previously notified status {} to a valid JobStatus",
                    jobEntity.getStatus(),
                    e
                );
                return;
            }
        } else {
            previouslyNotifiedState = null;
        }
        final String jobId = jobEntity.getUniqueId();
        if (currentState != previouslyNotifiedState) {
            log.debug(
                "Detected state change for job: {} from: {} to: {}",
                jobId,
                previouslyNotifiedState,
                currentState
            );

            // Notify observer
            this.persistedJobStatusObserver.notify(jobId, previouslyNotifiedState, currentState);

            // Save this as the latest published state
            jobEntity.setNotifiedJobStatus(currentState.name());
        }
    }

    /**
     * Persistence callback invoked after a job entity is loaded or refreshed.
     * The job status loaded from persistent storage is also the last state that was notified.
     *
     * @param jobEntity the job that was just loaded
     */
    @PostLoad
    public void jobLoad(final JobEntity jobEntity) {
        // The persisted status is also the most recently notified state.
        jobEntity.setNotifiedJobStatus(jobEntity.getStatus());
    }
}
