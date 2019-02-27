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

package com.netflix.genie.web.services.impl

import com.netflix.genie.common.internal.exceptions.StreamUnavailableException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.AgentFileStreamService
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

@Category(UnitTest)
class AgentFileStreamServiceImplSpec extends Specification {
    AgentFileStreamServiceImpl service
    static final String JOB_ID = "123456"
    static final Path PATH = Paths.get("foo/bar/file.txt")
    static final int START_OFFSET = 0
    static final int END_OFFSET = 1000
    OutputStream outputStream

    void setup() {
        this.outputStream = Mock(OutputStream)
        this.service = new AgentFileStreamServiceImpl()
    }

    def "Register 2, use both, fail to get a third"() {
        setup:
        AgentFileStreamService.ReadyStream readyStream1 = Mock()
        AgentFileStreamService.ActiveStream activeStream1 = Mock()
        AgentFileStreamService.ReadyStream readyStream2 = Mock()
        AgentFileStreamService.ActiveStream activeStream2 = Mock()

        when:
        service.registerReadyStream(readyStream1)
        service.registerReadyStream(readyStream2)

        then:
        1 * readyStream1.getJobId() >> JOB_ID
        1 * readyStream2.getJobId() >> JOB_ID

        when:
        AgentFileStreamService.ActiveStream s1 = service.beginFileStream(JOB_ID, PATH, START_OFFSET, END_OFFSET)
        AgentFileStreamService.ActiveStream s2 = service.beginFileStream(JOB_ID, PATH, START_OFFSET, END_OFFSET)

        then:
        1 * readyStream1.activateStream(PATH, START_OFFSET, END_OFFSET) >> activeStream1
        1 * readyStream2.activateStream(PATH, START_OFFSET, END_OFFSET) >> activeStream2
        s1 == activeStream1
        s2 == activeStream2

        when:
        service.beginFileStream(JOB_ID, PATH, START_OFFSET, END_OFFSET)

        then:
        thrown(StreamUnavailableException)
    }

    def "Stream activation error"() {
        setup:
        AgentFileStreamService.ReadyStream readyStream1 = Mock()
        AgentFileStreamService.ReadyStream readyStream2 = Mock()
        AgentFileStreamService.ActiveStream activeStream = Mock()
        StreamUnavailableException exception = Mock()

        when:
        service.registerReadyStream(readyStream1)
        service.registerReadyStream(readyStream2)

        then:
        1 * readyStream1.getJobId() >> JOB_ID
        1 * readyStream2.getJobId() >> JOB_ID

        when:
        AgentFileStreamService.ActiveStream s = service.beginFileStream(JOB_ID, PATH, START_OFFSET, END_OFFSET)

        then:
        1 * readyStream1.activateStream(PATH, START_OFFSET, END_OFFSET) >> { throw exception }
        1 * readyStream2.activateStream(PATH, START_OFFSET, END_OFFSET) >> activeStream
        s == activeStream
    }

    def "Add then remove"() {
        setup:
        AgentFileStreamService.ReadyStream readyStream = Mock()
        StreamUnavailableException exception = Mock()

        when:
        service.registerReadyStream(readyStream)

        then:
        1 * readyStream.getJobId() >> JOB_ID

        when:
        service.unregisterReadyStream(readyStream)

        then:
        1 * readyStream.getJobId() >> JOB_ID

        when:
        service.beginFileStream(JOB_ID, PATH, START_OFFSET, END_OFFSET)

        then:
        thrown(StreamUnavailableException)
    }

}
