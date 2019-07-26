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
package com.netflix.genie.web.events

import com.netflix.genie.common.dto.JobStatus
import spock.lang.Specification
import spock.lang.Unroll

class JobStateChangeEventSpec extends Specification {

    String jobId = UUID.randomUUID().toString()

    @Unroll
    def "Construct with #prevStatus, #nextStatus"() {
        JobStateChangeEvent event = new JobStateChangeEvent(jobId, prevStatus, nextStatus, this)

        expect:
        event.getJobId() == jobId
        event.getNewStatus() == nextStatus
        event.getPreviousStatus() == prevStatus
        event.getSource() == this

        where:
        prevStatus         | nextStatus
        null               | JobStatus.RESERVED
        JobStatus.RESERVED | JobStatus.ACCEPTED

    }

}
