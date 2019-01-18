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

package com.netflix.genie.agent.execution.exceptions

import spock.lang.Specification
import spock.lang.Unroll

class AgentExecutionExceptionsSpec extends Specification {

    final String MESSAGE = "message"
    final Throwable CAUSE = new IOException()

    @Unroll
    def "Constructors for exception class #aClass"(Class<? extends Throwable> aClass, boolean checked) {
        setup:
        Throwable exWithMessage = aClass.getConstructor(String.class).newInstance(MESSAGE)
        Throwable exWithMessageAndCause = aClass.getConstructor(String.class, Throwable.class).newInstance(MESSAGE, CAUSE)

        expect:
        Exception.isInstance(exWithMessage)
        Exception.isInstance(exWithMessageAndCause)
        exWithMessage.getMessage() == MESSAGE
        exWithMessageAndCause.getMessage() == MESSAGE
        exWithMessage.getCause() == null
        exWithMessageAndCause.getCause() == CAUSE
        RuntimeException.isInstance(exWithMessage) != checked
        RuntimeException.isInstance(exWithMessageAndCause) != checked

        where:
        aClass                                    | checked
        DownloadException.class                   | true
        SetUpJobException.class                   | true
        JobLaunchException.class                  | true
        JobSpecificationResolutionException.class | true
        LockException.class                       | true
        JobIdUnavailableException.class           | true
        JobReservationException.class             | true
        ChangeJobStatusException.class            | true
        HandshakeException.class                  | true
        InvalidStateException.class               | false
    }
}
