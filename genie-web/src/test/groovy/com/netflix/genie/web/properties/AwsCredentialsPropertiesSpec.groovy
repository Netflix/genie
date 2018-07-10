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

import spock.lang.Specification

/**
 * Specifications for {@link AwsCredentialsProperties}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class AwsCredentialsPropertiesSpec extends Specification {

    def "Can get and set values"() {
        when:
        def properties = new AwsCredentialsProperties()

        then:
        properties.getRole() == null

        when:
        def newRole = UUID.randomUUID().toString()
        properties.setRole(newRole)

        then:
        properties.getRole() == newRole
    }
}
