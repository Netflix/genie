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

package com.netflix.genie.web.util

import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.scheduling.TriggerContext
import spock.lang.Specification
import spock.lang.Unroll

@Category(UnitTest)
class ExponentialBackOffTriggerSpec extends Specification {

    @Unroll
    def "NextExecutionTime with type #delayType"(
            ExponentialBackOffTrigger.DelayType delayType,
            long[] nextExpectedExecutions
    ) {
        setup:
        long min = 1L
        long max = 16L
        float factor = 2.0f
        TriggerContext triggerContext = Mock(TriggerContext) {
            lastScheduledExecutionTime() >> new Date(100)
            lastActualExecutionTime() >> new Date(200)
            lastCompletionTime() >> new Date(300)
        }
        ExponentialBackOffTrigger trigger = new ExponentialBackOffTrigger(
                delayType,
                min,
                max,
                factor
        )

        for (Long expectedDelay : nextExpectedExecutions) {
            when:
            Date nextExecutionTime = trigger.nextExecutionTime(triggerContext)

            then:
            Date expectedExecutionTime = new Date(expectedDelay)
            nextExecutionTime == expectedExecutionTime
        }

        trigger.reset()

        for (Long expectedDelay : nextExpectedExecutions) {
            when:
            Date nextExecutionTime = trigger.nextExecutionTime(triggerContext)

            then:
            Date expectedExecutionTime = new Date(expectedDelay)
            nextExecutionTime == expectedExecutionTime
        }

        where:
        delayType                                                              | nextExpectedExecutions
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_SCHEDULING           | [101, 102, 104, 108, 116, 116, 116]
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN      | [201, 202, 204, 208, 216, 216, 216]
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION | [301, 302, 304, 308, 316, 316, 316]
    }

    @Unroll
    def "NextExecutionTime with type #delayType and no previous execution"(
            ExponentialBackOffTrigger.DelayType delayType,
            List<Long> nextExpectedExecutions
    ) {
        setup:
        long min = 10_000L
        long max = 100_000L
        float factor = 4.0f
        TriggerContext triggerContext = Mock(TriggerContext) {
            lastScheduledExecutionTime() >> null
            lastActualExecutionTime() >> null
            lastCompletionTime() >> null
        }
        ExponentialBackOffTrigger trigger = new ExponentialBackOffTrigger(
                delayType,
                min,
                max,
                factor
        )

        for (Long expectedDelay : nextExpectedExecutions) {
            long now = System.currentTimeMillis()

            when:
            Date nextExecutionTime = trigger.nextExecutionTime(triggerContext)

            then:
            Date expectedExecutionTime = new Date(now + expectedDelay)
            assert approx(nextExecutionTime, expectedExecutionTime, 1_000L)
        }

        where:
        delayType                                                              | nextExpectedExecutions
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_SCHEDULING           | [10_000, 40_000, 100_000, 100_000]
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN      | [10_000, 40_000, 100_000, 100_000]
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_COMPLETION | [10_000, 40_000, 100_000, 100_000]
    }

    def approx(final Date date, final Date expectedDate, final long toleranceMillis) {
        return Math.abs(date.toInstant().toEpochMilli() - expectedDate.toInstant().toEpochMilli()) < toleranceMillis
    }

    @Unroll
    def "Reject invalid arguments: <#min, #max, #factor>" (
            long min,
            long max,
            float factor
    ) {
        when:
        new ExponentialBackOffTrigger(
                ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_SCHEDULING,
                min,
                max,
                factor
        )

        then:
        thrown(IllegalArgumentException)

        where:

        min | max | factor
        -1  | 100 | 2.0f
        0   | 100 | 2.0f
        10  | 9   | 2.0f
        10  | 100 | -2.0f
        10  | 100 | 0.0f
    }
}
