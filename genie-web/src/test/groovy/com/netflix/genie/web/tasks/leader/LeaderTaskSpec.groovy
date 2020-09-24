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
package com.netflix.genie.web.tasks.leader

import com.netflix.genie.web.tasks.GenieTaskScheduleType
import spock.lang.Specification

class LeaderTaskSpec extends Specification {

    def "Test simple concrete subclass"() {
        setup:
        LeaderTask task = new TestLeaderTask()

        when:
        task.run()

        then:
        task.runCount == 1

        when:
        task.getTrigger()

        then:
        thrown(UnsupportedOperationException)

        when:
        task.getFixedRate()

        then:
        thrown(UnsupportedOperationException)

        when:
        task.getFixedDelay()

        then:
        thrown(UnsupportedOperationException)

        when:
        task.cleanup()

        then:
        noExceptionThrown()

        expect:
        task.getScheduleType() == GenieTaskScheduleType.FIXED_DELAY
    }

    private static class TestLeaderTask extends LeaderTask {

        def runCount = 0

        @Override
        GenieTaskScheduleType getScheduleType() {
            return GenieTaskScheduleType.FIXED_DELAY
        }

        @Override
        void run() {
            runCount++
        }
    }
}
