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
import com.amazonaws.regions.Regions
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.properties.AwsCredentialsProperties
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link GenieAwsS3AutoConfiguration} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class GenieAwsS3AutoConfigurationSpec extends Specification {

    def "Can get default S3 client"() {
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def clientConfiguration = new ClientConfiguration()
        String roleArn = null
        def credentialsProperties = new AwsCredentialsProperties()
        credentialsProperties.setRole(roleArn)
        def awsRegionProperties = new AwsCredentialsProperties.SpringCloudAwsRegionProperties()
        def config = new GenieAwsS3AutoConfiguration()

        when:
        def client = config.amazonS3(
                credentialsProvider,
                clientConfiguration,
                awsRegionProperties,
                credentialsProperties
        )

        then:
        client != null
        client.getRegionName() == Regions.US_EAST_1.getName()
    }

    def "Can can get S3 client with assumed role"() {
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def clientConfiguration = new ClientConfiguration()
        def roleArn = UUID.randomUUID().toString()
        def credentialsProperties = new AwsCredentialsProperties()
        credentialsProperties.setRole(roleArn)
        def awsRegionProperties = new AwsCredentialsProperties.SpringCloudAwsRegionProperties()
        awsRegionProperties.setStatic(Regions.US_WEST_1.getName())
        def config = new GenieAwsS3AutoConfiguration()

        when:
        def client = config.amazonS3(
                credentialsProvider,
                clientConfiguration,
                awsRegionProperties,
                credentialsProperties
        )

        then:
        client != null
        client.getRegionName() == Regions.US_WEST_1.getName()
    }
}
