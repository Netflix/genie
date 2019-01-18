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
import com.netflix.genie.common.internal.services.impl.NoOpJobArchiveServiceImpl
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory
import com.netflix.genie.common.internal.services.impl.S3JobArchiveServiceImpl
import spock.lang.Specification

/**
 * Specifications for {@link AgentAwsAutoConfiguration}.
 *
 * @author tgianos
 */
class AgentAwsAutoConfigurationSpec extends Specification {

    def "Can create correct archival service based on valid credentials or not"() {
        def config = new AgentAwsAutoConfiguration()
        def awsCredentialsProvider = Mock(AWSCredentialsProvider)
        def s3ClientFactory = Mock(S3ClientFactory)

        when:
        def archivalService = config.archivalService(awsCredentialsProvider, s3ClientFactory)

        then:
        1 * awsCredentialsProvider.getCredentials() >> { throw new SdkClientException("bad credentials") }
        archivalService instanceof NoOpJobArchiveServiceImpl

        when:
        archivalService = config.archivalService(awsCredentialsProvider, s3ClientFactory)

        then:
        1 * awsCredentialsProvider.getCredentials() >> Mock(AWSCredentials)
        archivalService instanceof S3JobArchiveServiceImpl
    }
}
