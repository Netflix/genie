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
package com.netflix.genie.agent.cli

import com.fasterxml.jackson.core.JsonProcessingException
import com.netflix.genie.agent.execution.exceptions.JobSpecificationResolutionException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.util.GenieObjectMapper
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class ResolveJobSpecCommandSpec extends Specification {

    @TempDir
    Path temporaryFolder

    ResolveJobSpecCommand.ResolveJobSpecCommandArguments commandArgs
    AgentJobService service
    ResolveJobSpecCommand command
    JobSpecification spec
    JobSpecificationResolutionException resolutionException
    ArgumentDelegates.JobRequestArguments jobArgs
    AgentJobRequest jobRequest
    JobRequestConverter jobRequestConverter

    void setup() {
        commandArgs = Mock()
        service = Mock()
        jobRequestConverter = Mock()
        command = new ResolveJobSpecCommand(commandArgs, service, jobRequestConverter)
        spec = Mock()
        resolutionException = Mock()
        jobArgs = Mock()
        jobRequest = Mock()
    }

    def "Resolve spec"() {
        setup:

        when:
        ExitCode exitCode = command.run()

        then:
        1 * commandArgs.getSpecificationId() >> " "
        1 * commandArgs.getJobRequestArguments() >> jobArgs
        1 * jobRequestConverter.agentJobRequestArgsToDTO(jobArgs) >> jobRequest
        1 * service.resolveJobSpecificationDryRun(jobRequest) >> spec
        1 * commandArgs.isPrintRequestDisabled() >> true
        1 * commandArgs.getOutputFile() >> null
        exitCode == ExitCode.SUCCESS
    }

    def "Resolve spec service error"() {
        setup:

        when:
        command.run()

        then:
        1 * commandArgs.getSpecificationId() >> " "
        1 * commandArgs.getJobRequestArguments() >> jobArgs
        1 * jobRequestConverter.agentJobRequestArgsToDTO(jobArgs) >> jobRequest
        1 * service.resolveJobSpecificationDryRun(jobRequest) >> { throw resolutionException }
        def e = thrown(RuntimeException)
        e.getCause() == resolutionException
    }

    def "Get spec by id"() {
        setup:
        String specId = "12345"

        when:
        ExitCode exitCode = command.run()

        then:
        1 * commandArgs.getSpecificationId() >> specId
        1 * service.getJobSpecification(specId) >> spec
        1 * commandArgs.getOutputFile() >> null
        exitCode == ExitCode.SUCCESS
    }

    def "Get spec by id error"() {
        setup:
        String specId = "12345"

        when:
        command.run()

        then:
        1 * commandArgs.getSpecificationId() >> specId
        1 * service.getJobSpecification(specId) >> { throw resolutionException }
        Throwable t = thrown(RuntimeException)
        t.getCause() == resolutionException
    }

    def "Write spec to file"() {
        setup:
        String specId = "12345"
        File outputFile = this.temporaryFolder.resolve("spec.json").toFile()

        when:
        ExitCode exitCode = command.run()

        then:
        1 * commandArgs.getSpecificationId() >> specId
        1 * service.getJobSpecification(specId) >> spec
        1 * commandArgs.getOutputFile() >> outputFile
        outputFile.exists()
        GenieObjectMapper.getMapper().readValue(outputFile.getText(), JobSpecification.class)
        exitCode == ExitCode.SUCCESS
    }

    def "Write spec to file that exists"() {
        setup:
        String specId = "12345"
        File outputFile = Files.createFile(this.temporaryFolder.resolve(UUID.randomUUID().toString())).toFile()

        when:
        command.run()

        then:
        1 * commandArgs.getSpecificationId() >> specId
        1 * service.getJobSpecification(specId) >> spec
        1 * commandArgs.getOutputFile() >> outputFile
        outputFile.exists()
        thrown(RuntimeException)
    }

    def "JSON request error"() {
        setup:

        when:
        command.run()

        then:
        1 * commandArgs.getSpecificationId() >> " "
        1 * commandArgs.getJobRequestArguments() >> jobArgs
        1 * jobRequestConverter.agentJobRequestArgsToDTO(jobArgs) >> jobRequest
        1 * commandArgs.isPrintRequestDisabled() >> false
        1 * jobRequest.getCommandArgs() >> { throw new IOException("") }
        def e = thrown(RuntimeException)
        e.getCause() instanceof JsonProcessingException
    }

    def "JSON response error"() {
        setup:

        when:
        command.run()

        then:
        1 * commandArgs.getSpecificationId() >> " "
        1 * commandArgs.getJobRequestArguments() >> jobArgs
        1 * jobRequestConverter.agentJobRequestArgsToDTO(jobArgs) >> jobRequest
        1 * service.resolveJobSpecificationDryRun(jobRequest) >> spec
        1 * commandArgs.isPrintRequestDisabled() >> false
        1 * spec.getExecutableArgs() >> { throw new IOException("") }
        def e = thrown(RuntimeException)
        e.getCause() instanceof JsonProcessingException
    }

}
