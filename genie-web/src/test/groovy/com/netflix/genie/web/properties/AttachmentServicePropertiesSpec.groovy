/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.properties

import org.springframework.util.unit.DataSize
import spock.lang.Specification

class AttachmentServicePropertiesSpec extends Specification {

    def "Defaults, getters, setters"() {
        when:
        AttachmentServiceProperties props = new AttachmentServiceProperties()

        then:
        props.getLocationPrefix().toString() ==~ /file:\/\/\/.+\/genie\/attachments/
        props.getMaxSize() == DataSize.ofMegabytes(100)
        props.getMaxTotalSize() == DataSize.ofMegabytes(150)

        when:
        props.setLocationPrefix(URI.create("s3://genie-attachments/prod"))
        props.setMaxSize(DataSize.ofMegabytes(50))
        props.setMaxTotalSize(DataSize.ofMegabytes(75))

        then:
        props.getLocationPrefix() == URI.create("s3://genie-attachments/prod")
        props.getMaxSize() == DataSize.ofMegabytes(50)
        props.getMaxTotalSize() == DataSize.ofMegabytes(75)
    }
}
