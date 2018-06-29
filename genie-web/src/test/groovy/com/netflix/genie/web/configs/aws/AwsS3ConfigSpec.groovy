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
package com.netflix.genie.web.configs.aws

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification
/**
 * Specifications for the {@link AwsS3Config} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class AwsS3ConfigSpec extends Specification {

    def "Can get default S3 client"() {
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def clientConfiguration = new ClientConfiguration()
        def region = "us-east-1"
        String roleArn = null
        def config = new AwsS3Config()

        when:
        def client = config.amazonS3(credentialsProvider, clientConfiguration, region, roleArn)

        then:
        client != null
        client.getRegionName() == region
    }

    def "Can can get S3 client with assumed role"() {
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def clientConfiguration = new ClientConfiguration()
        def region = "us-east-1"
        def roleArn = UUID.randomUUID().toString()
        def config = new AwsS3Config()

        when:
        def client = config.amazonS3(credentialsProvider, clientConfiguration, region, roleArn)

        then:
        client != null
        client.getRegionName() == region
    }
}
