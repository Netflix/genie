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
package com.netflix.genie.web.data.observers

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.web.events.GenieEventBus
import com.netflix.genie.web.events.JobStateChangeEvent
import org.springframework.context.ApplicationEvent
import spock.lang.Specification
import spock.lang.Unroll

class PersistedJobStatusObserverImplSpec extends Specification {

    GenieEventBus genieEventbus
    String jobId

    void setup() {
        this.genieEventbus = Mock(GenieEventBus)
        this.jobId = UUID.randomUUID().toString()
    }

    @Unroll
    def "Notify #prevStatus -> #currStatus"() {
        setup:
        PersistedJobStatusObserver observer = new PersistedJobStatusObserverImpl(genieEventbus)
        JobStateChangeEvent event

        when:
        observer.notify(jobId, prevStatus, currStatus)

        then:
        1 * genieEventbus.publishAsynchronousEvent(_ as ApplicationEvent) >> {
            args ->
                event = args[0] as JobStateChangeEvent
        }
        event != null
        event.getSource() == observer
        event.getPreviousStatus() == prevStatus
        event.getNewStatus() == currStatus
        event.getJobId() == jobId

        where:
        prevStatus         | currStatus
        null               | JobStatus.RESERVED
        JobStatus.RESERVED | JobStatus.RESOLVED
    }
}
