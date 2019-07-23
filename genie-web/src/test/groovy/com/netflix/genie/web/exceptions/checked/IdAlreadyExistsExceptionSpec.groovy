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

import org.springframework.dao.DataIntegrityViolationException
import spock.lang.Specification

/**
 * Specifications for {@link IdAlreadyExistsException}.
 *
 * @author tgianos
 */
class IdAlreadyExistsExceptionSpec extends Specification {

    def "can construct with message"() {
        def message = "id exists"

        when:
        def exception = new IdAlreadyExistsException(message)

        then:
        exception.getMessage() == message
        exception.getCause() == null
    }

    def "can construct with cause"() {
        def cause = new IllegalStateException("some bad state")

        when:
        def exception = new IdAlreadyExistsException(cause)

        then:
        exception.getMessage() != null
        exception.getCause() == cause
    }

    def "can construct with message and cause"() {
        def message = "id exists"
        def cause = new DataIntegrityViolationException("no process")

        when:
        def exception = new IdAlreadyExistsException(message, cause)

        then:
        exception.getMessage() == message
        exception.getCause() == cause
    }
}
