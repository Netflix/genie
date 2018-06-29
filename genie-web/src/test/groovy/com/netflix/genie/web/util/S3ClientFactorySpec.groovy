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
package com.netflix.genie.web.util

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.securitytoken.model.AWSSecurityTokenServiceException
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for {@link S3ClientFactory}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class S3ClientFactorySpec extends Specification {

    def "Can get default S3 client"() {
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def clientConfiguration = new ClientConfiguration()
        def region = "us-east-1"
        String roleArn = null

        def factory = new S3ClientFactory(
                credentialsProvider,
                clientConfiguration,
                region,
                roleArn
        )

        when:
        def client = factory.getS3Client()

        then:
        client != null
        client.getRegionName() == region
    }

    def "Can can get S3 client with assumed role"() {
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def clientConfiguration = new ClientConfiguration()
        def region = "us-east-1"
        def roleArn = UUID.randomUUID().toString()

        def factory = new S3ClientFactory(
                credentialsProvider,
                clientConfiguration,
                region,
                roleArn
        )

        when:
        factory.getS3Client()

        then:
        thrown(AWSSecurityTokenServiceException)
    }
}
