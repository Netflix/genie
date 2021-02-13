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

import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata
import spock.lang.Specification

/**
 * Specifications for {@link JobSubmission}.
 *
 * @author tgianos
 */
class JobSubmissionSpec extends Specification {

    def "Can build"() {
        def jobRequest = Mock(JobRequest)
        def jobRequestMetadata = Mock(JobRequestMetadata)
        def attachment1 = URI.create("s3://some-bucket/scripts/script1.sql")
        def attachment2 = URI.create("s3://some-bucket/scripts/script2.sql")

        def builder = new JobSubmission.Builder(jobRequest, jobRequestMetadata)

        when:
        def submission1 = builder.build()

        then:
        submission1.getJobRequest() == jobRequest
        submission1.getJobRequestMetadata() == jobRequestMetadata
        submission1.getAttachments().isEmpty()

        when:
        def submission2 = builder.withAttachments(attachment1, attachment2).build()

        then:
        submission2.getJobRequest() == jobRequest
        submission2.getJobRequestMetadata() == jobRequestMetadata
        submission2.getAttachments().size() == 2
        submission2.getAttachments().containsAll([attachment1, attachment2])
        submission1.toString() != submission2.toString()
        submission1.hashCode() != submission2.hashCode()
        submission1 != submission2
        submission1.getAttachments() != submission2.getAttachments()

        when:
        def submission3 = builder.withAttachments([attachment1, attachment2].toSet()).build()

        then:
        submission3.getJobRequest() == jobRequest
        submission3.getJobRequestMetadata() == jobRequestMetadata
        submission3.getAttachments().size() == 2
        submission3.getAttachments().containsAll([attachment1, attachment2])
        // note the attachments are ignored
        submission2.toString() == submission3.toString()
        submission2.hashCode() == submission3.hashCode()
        submission2 == submission3
        submission2.getAttachments() == submission3.getAttachments()

        when:
        def submission4 = builder.withAttachments(null).build()

        then:
        submission4.getAttachments().isEmpty()
    }
}
