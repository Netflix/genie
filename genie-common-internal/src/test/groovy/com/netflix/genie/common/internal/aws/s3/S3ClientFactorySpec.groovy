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
package com.netflix.genie.common.internal.aws.s3

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.AwsRegionProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3URI
import org.springframework.mock.env.MockEnvironment
import spock.lang.Specification

/**
 * Specifications for {@link S3ClientFactory}.
 *
 * @author tgianos
 * @since
 */
class S3ClientFactorySpec extends Specification {

    def "Can construct with empty bucket mapping properties"() {
        def environment = new MockEnvironment()
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)

        when:
        def factory = new S3ClientFactory(credentialsProvider, regionProvider, environment)

        then:
        1 * regionProvider.getRegion() >> Regions.US_EAST_1.getName()
        factory.defaultRegion == Regions.US_EAST_1
        factory.bucketProperties.isEmpty()
        factory.stsClient != null
        factory.bucketToClientKey.isEmpty()
        factory.clientCache.isEmpty()
    }

    def "Can construct with bucket mapping properties"() {
        def environment = new MockEnvironment()
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)

        def bucket0Name = UUID.randomUUID().toString()
        def bucket0Role = "arn:aws:iam::accountNumber:role/someRole"

        def bucket1Name = UUID.randomUUID().toString()
        def bucket1Region = Regions.EU_WEST_2.getName()

        def bucket2Name = UUID.randomUUID().toString()
        def bucket2Role = "arn:aws:iam::accountNumber:role/someRole"
        def bucket2Region = Regions.US_WEST_2.getName()

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket0Name + ".roleARN",
            bucket0Role
        )

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket1Name + ".region",
            bucket1Region
        )

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket2Name + ".roleARN",
            bucket2Role
        )
        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket2Name + ".region",
            bucket2Region
        )

        when:
        def factory = new S3ClientFactory(credentialsProvider, regionProvider, environment)

        then:
        1 * regionProvider.getRegion() >> Regions.US_EAST_1.getName()
        factory.defaultRegion == Regions.US_EAST_1
        factory.bucketProperties.size() == 3
        factory.bucketProperties.containsKey(bucket0Name)
        !factory.bucketProperties.get(bucket0Name).getRegion().isPresent()
        factory.bucketProperties.get(bucket0Name).getRoleARN().orElse(null) == bucket0Role
        factory.bucketProperties.containsKey(bucket1Name)
        factory.bucketProperties.get(bucket1Name).getRegion().orElse(null) == bucket1Region
        !factory.bucketProperties.get(bucket1Name).getRoleARN().isPresent()
        factory.bucketProperties.containsKey(bucket2Name)
        factory.bucketProperties.get(bucket2Name).getRegion().orElse(null) == bucket2Region
        factory.bucketProperties.get(bucket2Name).getRoleARN().orElse(null) == bucket2Role
        factory.stsClient != null
        factory.bucketToClientKey.isEmpty()
        factory.clientCache.isEmpty()
    }

    def "Can get clients for various scenarios"() {
        def environment = new MockEnvironment()
        def credentialsProvider = Mock(AWSCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)

        def bucket0Name = UUID.randomUUID().toString()
        def bucket0Role = "arn:aws:iam::accountNumber:role/someRole"

        def bucket1Name = UUID.randomUUID().toString()
        def bucket1Region = Regions.EU_WEST_2.getName()

        def bucket2Name = UUID.randomUUID().toString()
        def bucket2Role = "arn:aws:iam::accountNumber:role/someRole"
        def bucket2Region = Regions.US_WEST_2.getName()

        def bucket3Name = UUID.randomUUID().toString()
        def bucket3Role = "arn:aws:iam::accountNumber:role2/someRole2"
        def bucket3Region = Regions.US_WEST_2.getName()

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket0Name + ".roleARN",
            bucket0Role
        )

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket1Name + ".region",
            bucket1Region
        )

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket2Name + ".roleARN",
            bucket2Role
        )
        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket2Name + ".region",
            bucket2Region
        )

        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket3Name + ".roleARN",
            bucket3Role
        )
        environment.withProperty(
            S3ClientFactory.BUCKET_PROPERTIES_ROOT_KEY + "." + bucket3Name + ".region",
            bucket3Region
        )

        def s3URI = Mock(AmazonS3URI)
        def bucket4Name = UUID.randomUUID().toString()

        when:
        def factory = new S3ClientFactory(credentialsProvider, regionProvider, environment)

        then:
        1 * regionProvider.getRegion() >> Regions.US_EAST_1.getName()
        factory.stsClient != null
        factory.bucketToClientKey.isEmpty()
        factory.clientCache.isEmpty()

        when: "A client is requested for a bucket and region combination"
        def amazonS3Client0 = factory.getClient(s3URI)

        then: "A default role client is created and returned for the region"
        1 * s3URI.getBucket() >> bucket4Name
        1 * s3URI.getRegion() >> Regions.EU_CENTRAL_1.getName()
        factory.bucketToClientKey.size() == 1
        factory.clientCache.size() == 1

        when: "The same bucket is requested"
        def amazonS3Client1 = factory.getClient(s3URI)

        then: "The same client is returned"
        1 * s3URI.getBucket() >> bucket4Name
        0 * s3URI.getRegion()
        factory.bucketToClientKey.size() == 1
        factory.clientCache.size() == 1
        amazonS3Client0 == amazonS3Client1

        when: "A different bucket in the same region is requested"
        def amazonS3Client2 = factory.getClient(s3URI)

        then: "The same client is returned"
        1 * s3URI.getBucket() >> UUID.randomUUID().toString()
        1 * s3URI.getRegion() >> Regions.EU_CENTRAL_1.getName()
        factory.bucketToClientKey.size() == 2
        factory.clientCache.size() == 1
        amazonS3Client0 == amazonS3Client2

        when: "A bucket which needs a role is requested"
        def amazonS3Client3 = factory.getClient(s3URI)

        then: "A new client with assume role is created"
        1 * s3URI.getBucket() >> bucket2Name
        1 * s3URI.getRegion() >> null
        factory.bucketToClientKey.size() == 3
        factory.clientCache.size() == 2
        amazonS3Client0 != amazonS3Client3

        when: "Same role and region but different bucket"
        def amazonS3Client4 = factory.getClient(s3URI)

        then: "The same client is returned"
        1 * s3URI.getBucket() >> bucket0Name
        1 * s3URI.getRegion() >> Regions.US_WEST_2.getName()
        factory.bucketToClientKey.size() == 4
        factory.clientCache.size() == 2
        amazonS3Client0 != amazonS3Client4
        amazonS3Client3 == amazonS3Client4

        when: "No bucket region or role are provided"
        def amazonS3Client5 = factory.getClient(s3URI)

        then: "The default region is used in a new client"
        1 * s3URI.getBucket() >> UUID.randomUUID().toString()
        1 * s3URI.getRegion() >> null
        factory.bucketToClientKey.size() == 5
        factory.clientCache.size() == 3
        amazonS3Client0 != amazonS3Client5
        amazonS3Client3 != amazonS3Client5

        when: "Another bucket with no region or role information is requested"
        def amazonS3Client6 = factory.getClient(s3URI)

        then: "The same default client is returned"
        1 * s3URI.getBucket() >> UUID.randomUUID().toString()
        1 * s3URI.getRegion() >> null
        factory.bucketToClientKey.size() == 6
        factory.clientCache.size() == 3
        amazonS3Client0 != amazonS3Client6
        amazonS3Client3 != amazonS3Client6
        amazonS3Client5 == amazonS3Client6

        when: "A bucket is requested in a region that already has a client but it's a different role than before"
        def amazonS3Client7 = factory.getClient(s3URI)

        then: "A new client is returned"
        1 * s3URI.getBucket() >> bucket3Name
        1 * s3URI.getRegion() >> null
        factory.bucketToClientKey.size() == 7
        factory.clientCache.size() == 4
        amazonS3Client3 != amazonS3Client7
    }
}
