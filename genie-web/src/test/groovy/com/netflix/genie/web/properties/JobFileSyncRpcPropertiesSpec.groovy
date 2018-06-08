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
package com.netflix.genie.web.properties

import com.netflix.genie.test.suppliers.RandomSuppliers
import spock.lang.Specification

/**
 * Specifications for the {@link JobFileSyncRpcProperties} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class JobFileSyncRpcPropertiesSpec extends Specification {

    def "Default parameters are as expected"() {
        when:
        def properties = new JobFileSyncRpcProperties()

        then:
        properties.getAckIntervalMilliseconds() == 30_000L
        properties.getMaxSyncMessages() == 10
    }

    def "Can set new acknowledgement interval"() {
        when:
        def properties = new JobFileSyncRpcProperties()

        then:
        properties.getAckIntervalMilliseconds() == 30_000L

        when:
        def newAck = RandomSuppliers.LONG.get()
        properties.setAckIntervalMilliseconds(newAck)

        then:
        properties.getAckIntervalMilliseconds() == newAck
    }

    def "Can set new max sync messages"() {
        when:
        def properties = new JobFileSyncRpcProperties()

        then:
        properties.getMaxSyncMessages() == 10

        when:
        def newMax = RandomSuppliers.INT.get()
        properties.setMaxSyncMessages(newMax)

        then:
        properties.getMaxSyncMessages() == newMax
    }
}
