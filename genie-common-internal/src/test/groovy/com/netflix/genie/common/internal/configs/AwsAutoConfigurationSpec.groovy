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
package com.netflix.genie.common.internal.configs

import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.regions.Regions
import io.awspring.cloud.autoconfigure.context.properties.AwsRegionProperties
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link AwsAutoConfiguration}.
 *
 * @author tgianos
 */
class AwsAutoConfigurationSpec extends Specification {

    @Unroll
    def "Can create the expected AwsRegionProvider instance when static is #staticRegion"() {
        def config = new AwsAutoConfiguration()
        def properties = new AwsRegionProperties()

        when:
        properties.setStatic(staticRegion)
        def regionProvider = config.awsRegionProvider(properties)

        then:
        if (!(regionProvider instanceof DefaultAwsRegionProviderChain)) {
            regionProvider.getRegion() == expectedRegion
        } else {
            // We expect the default to be returned when these conditions are true
            staticRegion == null
        }

        where:
        staticRegion                | expectedRegion
        Regions.US_WEST_2.getName() | Regions.US_WEST_2.getName()
        null                        | "shouldn't matter"
    }
}
