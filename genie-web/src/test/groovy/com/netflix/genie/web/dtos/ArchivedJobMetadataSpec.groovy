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

import com.netflix.genie.common.internal.dto.DirectoryManifest
import spock.lang.Specification
/**
 * Specifications for {@link ArchivedJobMetadata}.
 *
 * @author tgianos
 */
class ArchivedJobMetadataSpec extends Specification {

    def "can create and do all POJO operations"() {
        def jobId = UUID.randomUUID().toString()
        def manifest = Mock(DirectoryManifest)
        def jobDirectoryRoot = URI.create("file:/tmp/blah")

        when:
        def metadata = new ArchivedJobMetadata(jobId, manifest, jobDirectoryRoot)

        then:
        metadata.getJobId() == jobId
        metadata.getManifest() == manifest
        metadata.getJobDirectoryRoot() == jobDirectoryRoot

        when:
        def metadata2 = new ArchivedJobMetadata(
            UUID.randomUUID().toString(),
            Mock(DirectoryManifest),
            URI.create("http://someurl.com")
        )
        def metadata3 = new ArchivedJobMetadata(jobId, manifest, jobDirectoryRoot)

        then:
        metadata != metadata2
        metadata == metadata3
        metadata.hashCode() != metadata2.hashCode()
        metadata.hashCode() == metadata3.hashCode()
        metadata.toString() != metadata2.toString()
        metadata.toString() == metadata3.toString()
    }
}
