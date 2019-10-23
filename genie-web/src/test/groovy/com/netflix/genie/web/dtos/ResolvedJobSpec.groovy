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
package com.netflix.genie.web.dtos

import com.netflix.genie.common.internal.dto.v4.JobEnvironment
import com.netflix.genie.common.internal.dto.v4.JobMetadata
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import spock.lang.Specification

/**
 * Specifications for {@link ResolvedJob}.
 *
 * @author tgianos
 */
class ResolvedJobSpec extends Specification {

    def "can create and do all POJO operations"() {
        def jobSpecification = Mock(JobSpecification)
        def jobEnvironment = Mock(JobEnvironment)
        def jobMetadata = Mock(JobMetadata)

        when:
        def resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, jobMetadata)

        then:
        resolvedJob.getJobSpecification() == jobSpecification
        resolvedJob.getJobEnvironment() == jobEnvironment
        resolvedJob.getJobMetadata() == jobMetadata

        when:
        def resolvedJob2 = new ResolvedJob(Mock(JobSpecification), Mock(JobEnvironment), Mock(JobMetadata))
        def resolvedJob3 = new ResolvedJob(jobSpecification, jobEnvironment, jobMetadata)

        then:
        resolvedJob != resolvedJob2
        resolvedJob == resolvedJob3
        resolvedJob.hashCode() != resolvedJob2.hashCode()
        resolvedJob.hashCode() == resolvedJob3.hashCode()
        resolvedJob.toString() != resolvedJob2.toString()
        resolvedJob.toString() == resolvedJob3.toString()
    }
}
