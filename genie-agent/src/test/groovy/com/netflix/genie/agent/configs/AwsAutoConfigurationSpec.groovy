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
package com.netflix.genie.agent.configs

import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.regions.Regions
import com.netflix.genie.agent.aws.s3.S3ClientFactory
import com.netflix.genie.agent.execution.services.impl.NoOpArchivalServiceImpl
import com.netflix.genie.agent.execution.services.impl.S3ArchivalServiceImpl
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link AwsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class AwsAutoConfigurationSpec extends Specification {

    @Unroll
    def "Can create the expected AwsRegionProvider instance when auto is #auto and static is #staticRegion"() {
        def config = new AwsAutoConfiguration()

        when:
        def regionProvider = config.awsRegionProvider(auto, staticRegion)

        then:
        if (!(regionProvider instanceof DefaultAwsRegionProviderChain)) {
            regionProvider.getRegion() == expectedRegion
        } else {
            // We expect the default to be returned when these conditions are true
            auto
            staticRegion == null
        }

        where:
        auto  | staticRegion                | expectedRegion
        true  | Regions.US_WEST_2.getName() | Regions.US_WEST_2.getName()
        true  | null                        | "shouldn't matter"
        false | null                        | Regions.US_EAST_1.getName()
    }

    def "Can create correct archival service based on valid credentials or not"() {
        def config = new AwsAutoConfiguration()
        def awsCredentialsProvider = Mock(AWSCredentialsProvider)
        def s3ClientFactory = Mock(S3ClientFactory)

        when:
        def archivalService = config.archivalService(awsCredentialsProvider, s3ClientFactory)

        then:
        1 * awsCredentialsProvider.getCredentials() >> { throw new SdkClientException("bad credentials") }
        archivalService instanceof NoOpArchivalServiceImpl

        when:
        archivalService = config.archivalService(awsCredentialsProvider, s3ClientFactory)

        then:
        1 * awsCredentialsProvider.getCredentials() >> Mock(AWSCredentials)
        archivalService instanceof S3ArchivalServiceImpl
    }
}
