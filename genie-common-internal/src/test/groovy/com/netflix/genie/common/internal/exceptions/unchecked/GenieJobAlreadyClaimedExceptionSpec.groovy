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
package com.netflix.genie.common.internal.exceptions.unchecked

import spock.lang.Specification

/**
 * Specifications for the {@link GenieJobAlreadyClaimedException} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class GenieJobAlreadyClaimedExceptionSpec extends Specification {

    def "Can construct"() {
        String message = UUID.randomUUID().toString()
        Throwable cause = new Exception()
        GenieJobAlreadyClaimedException exception

        when:
        exception = new GenieJobAlreadyClaimedException()

        then:
        exception.getMessage() == null
        exception.getCause() == null

        when:
        exception = new GenieJobAlreadyClaimedException(message)

        then:
        exception.getMessage() == message
        exception.getCause() == null

        when:
        exception = new GenieJobAlreadyClaimedException(message, cause)

        then:
        exception.getMessage() == message
        exception.getCause() == cause

        when:
        exception = new GenieJobAlreadyClaimedException(cause)

        then:
        exception.getMessage() == cause.toString()
        exception.getCause() == cause
    }
}
