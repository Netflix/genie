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
package com.netflix.genie.web.exceptions.checked

import spock.lang.Specification

/**
 * Specifications for {@link SaveAttachmentException}.
 *
 * @author tgianos
 */
class SaveAttachmentExceptionSpec extends Specification {

    def "can construct with message"() {
        def message = "whoops"

        when:
        def exception = new SaveAttachmentException(message)

        then:
        exception.getMessage() == message
        exception.getCause() == null
    }

    def "can construct with cause"() {
        def cause = new IllegalStateException("some bad state")

        when:
        def exception = new SaveAttachmentException(cause)

        then:
        exception.getMessage() != null
        exception.getCause() == cause
    }

    def "can construct with message and cause"() {
        def message = "boo boo"
        def cause = new IOException("that didn't work")

        when:
        def exception = new SaveAttachmentException(message, cause)

        then:
        exception.getMessage() == message
        exception.getCause() == cause
    }
}
