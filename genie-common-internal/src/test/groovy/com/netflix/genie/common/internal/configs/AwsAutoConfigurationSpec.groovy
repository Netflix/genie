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

import io.awspring.cloud.autoconfigure.core.RegionProperties
import software.amazon.awssdk.regions.Region
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
        def properties = new RegionProperties()

        when:
        properties.setStatic(staticRegion)
        def regionProvider = config.awsRegionProvider(properties)

        then:
        if (staticRegion) {
            regionProvider.getRegion() == Region.of(staticRegion)
        } else {
            // For null static region, we should get a provider that either uses DefaultAwsRegionProviderChain
            // or falls back to US_EAST_1
            try {
                def region = regionProvider.getRegion()
                assert region != null
            } catch (Exception e) {
                // If DefaultAwsRegionProviderChain fails, we should fall back to US_EAST_1
                assert regionProvider.getRegion() == Region.US_EAST_1
            }
        }

        where:
        staticRegion    | _
        "us-west-2"     | _
        null            | _
    }
}
