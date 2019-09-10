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
package com.netflix.genie.agent.execution.process

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.JobStatusMessages
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link JobProcessResult}.
 *
 * @author tgianos
 */
class JobProcessResultSpec extends Specification {

    @Unroll
    def "can create with #finalStatus, #finalStatusMessage, #stdOutSize, #stdErrSize, #exitCode"() {
        when:
        def builder = new JobProcessResult.Builder(finalStatus, finalStatusMessage, exitCode)
        if (stdOutSize != null) {
            builder.withStdOutSize(stdOutSize)
        }
        if (stdErrSize != null) {
            builder.withStdErrSize(stdErrSize)
        }
        def result = builder.build()

        then:
        result.getFinalStatus() == finalStatus
        result.getFinalStatusMessage() == finalStatusMessage
        result.getStdOutSize() == expectedStdOutSize
        result.getStdErrSize() == expectedStdErrSize
        result.getExitCode() == exitCode

        where:
        finalStatus         | finalStatusMessage                          | stdOutSize | expectedStdOutSize | stdErrSize | expectedStdErrSize | exitCode
        JobStatus.SUCCEEDED | JobStatusMessages.JOB_FINISHED_SUCCESSFULLY | -1L        | 0L                 | 10L        | 10L                | 0
        JobStatus.FAILED    | JobStatusMessages.JOB_FAILED                | 1L         | 1L                 | -45L       | 0L                 | 1
        JobStatus.KILLED    | JobStatusMessages.JOB_KILLED_BY_USER        | null       | 0L                 | null       | 0L                 | -1
        JobStatus.INVALID   | "I was bad"                                 | null       | 0L                 | null       | 0L                 | 283
    }

    def "Preconditions throw expected exceptions"() {
        when:
        new JobProcessResult.Builder(JobStatus.RUNNING, "This will fail", 1)

        then:
        thrown(IllegalArgumentException)
    }
}
