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

package com.netflix.genie.agent.execution

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.v4.JobSpecification
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification
import spock.lang.Unroll

@Category(UnitTest.class)
class ExecutionContextImplSpec extends Specification {

    def "Get and set all"() {
        setup:
        ExecutionContext executionContext = new ExecutionContextImpl()
        String agentId = "foo"
        Process process = Mock()
        File directory = Mock()
        JobSpecification spec = Mock()
        Map<String, String> env = [ "foo": "bar" ]
        JobStatus finalJobStatus = JobStatus.SUCCEEDED

        expect:
        null == executionContext.getAgentId()
        null == executionContext.getJobProcess()
        null == executionContext.getJobDirectory()
        null == executionContext.getJobSpecification()
        null == executionContext.getJobEnvironment()
        null == executionContext.getFinalJobStatus()

        when:
        executionContext.setAgentId(agentId)
        executionContext.setJobProcess(process)
        executionContext.setJobDirectory(directory)
        executionContext.setJobSpecification(spec)
        executionContext.setJobEnvironment(env)
        executionContext.setFinalJobStatus(finalJobStatus)

        then:
        agentId == executionContext.getAgentId()
        process == executionContext.getJobProcess()
        directory == executionContext.getJobDirectory()
        spec == executionContext.getJobSpecification()
        env == executionContext.getJobEnvironment()
        finalJobStatus == executionContext.getFinalJobStatus()

        when:
        executionContext.setAgentId("bar")

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobProcess(Mock(Process))

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobDirectory(Mock(File))

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobSpecification(Mock(JobSpecification))

        then:
        thrown(RuntimeException)

        when:
        executionContext.setJobEnvironment(new HashMap<String, String>())

        then:
        thrown(RuntimeException)

        when:
        executionContext.setFinalJobStatus(JobStatus.FAILED)

        then:
        thrown(RuntimeException)
    }

    @Unroll
    def "Set final job status #jobStatus"(JobStatus jobStatus) {
        setup:
        ExecutionContext executionContext = new ExecutionContextImpl()

        when:
        executionContext.setFinalJobStatus(jobStatus)

        then:
        jobStatus == executionContext.getFinalJobStatus()

        where:
        jobStatus           | _
        JobStatus.FAILED    | _
        JobStatus.SUCCEEDED | _
        JobStatus.KILLED    | _
    }

    @Unroll
    def "Set invalid final job status #jobStatus"(JobStatus jobStatus) {
        setup:
        ExecutionContext executionContext = new ExecutionContextImpl()

        when:
        executionContext.setFinalJobStatus(jobStatus)

        then:
        thrown(IllegalArgumentException)

        where:
        jobStatus         | _
        JobStatus.INIT    | _
        JobStatus.RUNNING | _
    }
}
