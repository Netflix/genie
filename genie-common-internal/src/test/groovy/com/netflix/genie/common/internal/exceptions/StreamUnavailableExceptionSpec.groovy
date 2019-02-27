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

package com.netflix.genie.common.internal.exceptions

import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest)
class StreamUnavailableExceptionSpec extends Specification {
    static final String MESSAGE = "message"

    def "Construct"() {
        setup:
        IOException cause = new IOException("...")
        Exception e
        
        when:
        e = new StreamUnavailableException(MESSAGE)
        
        then:
        e.getMessage() == MESSAGE
        e.getCause() == null

        when:
        e = new StreamUnavailableException(MESSAGE, cause)

        then:\
        e.getMessage() == MESSAGE
        e.getCause() == cause
    }
}
