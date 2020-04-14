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
package com.netflix.genie.agent.execution.statemachine


import spock.lang.Specification


class FatalJobExecutionExceptionSpec extends Specification {
    void setup() {
    }

    def "Construct"() {
        setup:
        FatalJobExecutionException e1 = new FatalJobExecutionException(
            States.CREATE_JOB_DIRECTORY,
            "...",
            new IOException("...")
        )

        FatalJobExecutionException e2 = new FatalJobExecutionException(
            States.CREATE_JOB_DIRECTORY,
            "..."
        )

        expect:
        e1.getSourceState() == States.CREATE_JOB_DIRECTORY
        e1.getCause().getClass() == IOException
        e1.getMessage() == "..."

        e2.getSourceState() == States.CREATE_JOB_DIRECTORY
        e2.getCause() == null
        e2.getMessage() == "..."
    }
}
