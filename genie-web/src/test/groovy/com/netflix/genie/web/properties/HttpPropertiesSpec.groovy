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
 * Specifications for the {@link HttpProperties} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
class HttpPropertiesSpec extends Specification {

    def "Can get default properties"() {
        when:
        def properties = new HttpProperties()
        def connectProperties = properties.getConnect()
        def readProperties = properties.getRead()

        then:
        connectProperties.getTimeout() == 2_000
        readProperties.getTimeout() == 10_000

        when:
        def newConnectTimeout = RandomSuppliers.INT.get()
        def newReadTimeout = RandomSuppliers.INT.get()
        connectProperties.setTimeout(newConnectTimeout)
        readProperties.setTimeout(newReadTimeout)

        then:
        connectProperties.getTimeout() == newConnectTimeout
        readProperties.getTimeout() == newReadTimeout
    }
}
