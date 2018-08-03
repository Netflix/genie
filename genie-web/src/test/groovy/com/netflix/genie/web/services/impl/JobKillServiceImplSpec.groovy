/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.services.impl

import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.JobKillServiceV4
import com.netflix.genie.web.services.JobPersistenceService
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Tests for JobKillServiceImpl
 *
 * @author standon
 * @since 4.0.0
 */
@Category(UnitTest.class)
class JobKillServiceImplSpec extends Specification {
    JobKillServiceV3 jobKillServiceV3
    JobKillServiceImpl service
    JobPersistenceService jobPersistenceService
    JobKillServiceV4 jobKillServiceV4
    String jobId

    void setup() {
        jobPersistenceService = Mock()
        jobKillServiceV4 = Mock()
        jobKillServiceV3 = Mock()
        jobId = UUID.randomUUID().toString()
        service = new JobKillServiceImpl (jobKillServiceV3, jobKillServiceV4, jobPersistenceService)
    }

    def "Invoke JobKillServiceV3 for a v3 job"() {

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobPersistenceService.isV4(jobId) >> false
        0 * jobKillServiceV4.killJob(jobId, "testing")
        1 * jobKillServiceV3.killJob(jobId, "testing")
    }

    def "Invoke JobKillServiceV4 for a v4 job"() {

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobPersistenceService.isV4(jobId) >> true
        1 * jobKillServiceV4.killJob(jobId, "testing")
        0 * jobKillServiceV3.killJob(jobId, "testing")
    }

    def "Percolate up the exception thrown by underlying services when killing a job"() {

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobPersistenceService.isV4(jobId) >> { throw new GenieJobNotFoundException()}
        thrown(GenieJobNotFoundException)

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobPersistenceService.isV4(jobId) >> true
        1 * jobKillServiceV4.killJob(jobId, "testing") >> { throw new GenieServerException("Error killing v4 job")}
        thrown(GenieServerException)

        when:
        service.killJob(jobId, "testing")

        then:
        1 * jobPersistenceService.isV4(jobId) >> false
        1 * jobKillServiceV3.killJob(jobId, "testing") >> { throw new GenieServerException("Error killing v3 job")}
        thrown(GenieServerException)
    }
}
