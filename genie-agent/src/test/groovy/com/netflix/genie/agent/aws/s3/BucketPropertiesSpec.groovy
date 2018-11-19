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
package com.netflix.genie.agent.aws.s3

import com.amazonaws.regions.Regions
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link BucketProperties}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class BucketPropertiesSpec extends Specification {

    def "Can build with defaults"() {
        when:
        def properties = new BucketProperties()

        then:
        !properties.getRegion().isPresent()
        !properties.getRoleARN().isPresent()
    }

    def "Can set null values"() {
        when:
        def properties = new BucketProperties()
        properties.setRegion(null)
        properties.setRoleARN(null)

        then:
        !properties.getRegion().isPresent()
        !properties.getRoleARN().isPresent()
    }

    def "Can't set illegal region"() {
        def properties = new BucketProperties()

        when:
        properties.setRegion(UUID.randomUUID().toString())

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "Can't set illegal Role Arn #roleARN"() {
        def properties = new BucketProperties()

        when:
        properties.setRoleARN(roleARN)

        then:
        thrown(IllegalArgumentException)

        where:
        roleARN                                       | _
        UUID.randomUUID().toString()                  | _
        "arn:aws:notIAM::accountNumber:role/someRole" | _
    }

    def "Can set legal values"() {
        def properties = new BucketProperties()
        def roleARN = "arn:aws:iam::accountNumber:role/someRole"

        when:
        properties.setRegion(Regions.US_EAST_1.getName())
        properties.setRoleARN(roleARN)

        then:
        properties.getRegion().orElseThrow({ new IllegalArgumentException() }) == Regions.US_EAST_1.getName()
        properties.getRoleARN().orElseThrow({ new IllegalArgumentException() }) == roleARN
    }
}
