/*
 *
 *  Copyright 2016 Netflix, Inc.
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
import com.netflix.genie.web.services.FileTransferFactory
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit tests for CacheGenieFileTransferService.
 *
 * @author amajumdar
 */
@Unroll
class CacheGenieFileTransferServiceSpec extends Specification {
    LocalFileTransferImpl localFileTransfer = Mock(LocalFileTransferImpl)
    FileTransferFactory fileTransferFactory = Mock(FileTransferFactory) {
        get(_ as String) >> localFileTransfer
    }
    File cachedFile = Mock(File)
    MeterRegistry registry = new SimpleMeterRegistry()
    CacheGenieFileTransferService s =
        Spy(CacheGenieFileTransferService,
            constructorArgs: [fileTransferFactory, "/tmp", localFileTransfer, registry]) {
            createDirectories(_ as String) >> null
            deleteFile(_ as File) >> null
        }

    def 'Test getFile'() {
        when:
        s.getFile('file:/tmp/setup', 'file:/mnt/')
        then:
        noExceptionThrown()
        1 * s.loadFile(_) >> cachedFile
        when:
        s.getFile('file:/tmp/setup', 'file:/mnt/')
        then:
        noExceptionThrown()
        0 * s.loadFile(_) >> cachedFile
        when:
        s.getFile('file:/tmp/setup', 'file:/mnt/')
        then:
        noExceptionThrown()
        1 * s.loadFile(_) >> cachedFile
        2 * cachedFile.lastModified() >> -1
        when:
        s.getFile('file:/tmp/setup', 'file:/mnt/')
        then:
        noExceptionThrown()
        0 * s.loadFile(_) >> cachedFile
        1 * cachedFile.lastModified() >> 0
        when:
        s.getFile('file:/tmp/setup', 'file:/mnt/')
        then:
        thrown(GenieServerException)
        1 * s.loadFile(_) >> { throw new GenieServerException("null") }
        cachedFile.lastModified() >> -1
    }
}
