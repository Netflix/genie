/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.configs

import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.core.task.SyncTaskExecutor
import spock.lang.Specification

/**
 * Specification for the EventConfig class.
 *
 * @author tgianos
 * @since 3.1.2
 */
@Category(UnitTest.class)
class GenieEventBusAutoConfigurationSpec extends Specification {

    def "Can create Genie Event Bus"() {
        def config = new GenieEventBusAutoConfiguration()
        def syncExecutor = Mock(SyncTaskExecutor)
        def asyncExecutor = Mock(AsyncTaskExecutor)

        when:
        def eventBus = config.applicationEventMulticaster(syncExecutor, asyncExecutor)

        then:
        eventBus != null
    }
}
