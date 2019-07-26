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
package com.netflix.genie.web.spring.autoconfigure.events

import com.netflix.genie.web.data.observers.PersistedJobStatusObserver
import com.netflix.genie.web.events.GenieEventBus
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

class NotificationsAutoConfigurationSpec extends Specification {
    GenieEventBus genieEventBus
    NotificationsAutoConfiguration config
    MeterRegistry registry

    void setup() {
        this.genieEventBus = Mock(GenieEventBus)
        this.registry = Mock(MeterRegistry)
        this.config = new NotificationsAutoConfiguration()
    }

    def "persistedJobStatusObserver"() {
        PersistedJobStatusObserver observer

        when:
        observer = this.config.persistedJobStatusObserver(genieEventBus)

        then:
        observer != null
    }
}
