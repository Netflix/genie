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
package com.netflix.genie.web.data.entities.listeners

import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.web.data.entities.JobEntity
import com.netflix.genie.web.data.observers.PersistedJobStatusObserver
import spock.lang.Specification

class JobEntityListenerSpec extends Specification {
    String jobId
    PersistedJobStatusObserver observer
    JobEntity jobEntity

    void setup() {
        this.jobId = UUID.randomUUID().toString()
        this.observer = Mock(PersistedJobStatusObserver)
        this.jobEntity = Mock(JobEntity)
    }

    def "Can track job status and notify the observer"() {
        setup:
        JobEntityListener listener = new JobEntityListener(observer)

        when:
        listener.jobUpdate(jobEntity)

        then:
        1 * jobEntity.getStatus() >> JobStatus.RESERVED.name()
        1 * jobEntity.getNotifiedJobStatus() >> Optional.empty()
        1 * jobEntity.getUniqueId() >> jobId
        1 * observer.notify(jobId, null, JobStatus.RESERVED)
        1 * jobEntity.setNotifiedJobStatus(JobStatus.RESERVED.name())

        when:
        listener.jobLoad(jobEntity)

        then:
        1 * jobEntity.getStatus() >> JobStatus.RESERVED.name()
        1 * jobEntity.setNotifiedJobStatus(JobStatus.RESERVED.name())

        when:
        listener.jobUpdate(jobEntity)

        then:
        1 * jobEntity.getStatus() >> JobStatus.RESOLVED.name()
        1 * jobEntity.getNotifiedJobStatus() >> Optional.of(JobStatus.RESERVED.name())
        1 * jobEntity.getUniqueId() >> jobId
        1 * observer.notify(jobId, JobStatus.RESERVED, JobStatus.RESOLVED)
        1 * jobEntity.setNotifiedJobStatus(JobStatus.RESOLVED.name())

        when:
        listener.jobUpdate(jobEntity)

        then:
        1 * jobEntity.getStatus() >> JobStatus.RUNNING.name()
        1 * jobEntity.getNotifiedJobStatus() >> Optional.of(JobStatus.RUNNING.name())
        1 * jobEntity.getUniqueId() >> jobId
        0 * observer.notify(_, _, _)
        0 * jobEntity.setNotifiedJobStatus(_)
    }
}
