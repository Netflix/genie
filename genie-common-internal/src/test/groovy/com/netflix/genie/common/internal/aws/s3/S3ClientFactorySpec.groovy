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

import org.springframework.mock.env.MockEnvironment
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.AwsRegionProvider
import software.amazon.awssdk.services.s3.S3Uri
import spock.lang.Specification

/**
 * Specifications for {@link S3ClientFactory}.
 *
 * @author tgianos
 */
class S3ClientFactorySpec extends Specification {

    def "Can construct with empty bucket mapping properties"() {
        def environment = new MockEnvironment()
        def credentialsProvider = Mock(AwsCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)

        when:
        def factory = new S3ClientFactory(credentialsProvider, regionProvider, environment)

        then:
        1 * regionProvider.getRegion() >> Region.US_EAST_1
        factory.defaultRegion == Region.US_EAST_1
        factory.bucketProperties.isEmpty()
        factory.stsClient != null
        factory.bucketToClientKey.isEmpty()
        factory.clientCache.isEmpty()
    }

    def "Can construct with bucket mapping properties"() {
        def environment = new MockEnvironment()
        def credentialsProvider = Mock(AwsCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)

        def bucket0Name = UUID.randomUUID().toString()
        def bucket0Role = "arn:aws:iam::accountNumber:role/someRole"

        def bucket1Name = UUID.randomUUID().toString()
        def bucket1Region = Region.EU_WEST_2.id()

        def bucket2Name = UUID.randomUUID().toString()
        def bucket2Role = "arn:aws:iam::accountNumber:role/someRole"
        def bucket2Region = Region.US_WEST_2.id()

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
        1 * regionProvider.getRegion() >> Region.US_EAST_1
        factory.defaultRegion == Region.US_EAST_1
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
        def credentialsProvider = Mock(AwsCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)

        def bucket0Name = UUID.randomUUID().toString()
        def bucket0Role = "arn:aws:iam::accountNumber:role/someRole"

        def bucket1Name = UUID.randomUUID().toString()
        def bucket1Region = Region.EU_WEST_2.id()

        def bucket2Name = UUID.randomUUID().toString()
        def bucket2Role = "arn:aws:iam::accountNumber:role/someRole"
        def bucket2Region = Region.US_WEST_2.id()

        def bucket3Name = UUID.randomUUID().toString()
        def bucket3Role = "arn:aws:iam::accountNumber:role2/someRole2"
        def bucket3Region = Region.US_WEST_2.id()

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

        def s3Uri = Mock(S3Uri)
        def bucket4Name = UUID.randomUUID().toString()

        when:
        def factory = new S3ClientFactory(credentialsProvider, regionProvider, environment)

        then:
        1 * regionProvider.getRegion() >> Region.US_EAST_1
        factory.stsClient != null
        factory.bucketToClientKey.isEmpty()
        factory.clientCache.isEmpty()

        when: "A client is requested for a bucket and region combination"
        def s3Client0 = factory.getClient(s3Uri)

        then: "A default role client is created and returned for the region"
        1 * s3Uri.bucket() >> Optional.of(bucket4Name)
        1 * s3Uri.region() >> Optional.of(Region.EU_CENTRAL_1)
        factory.bucketToClientKey.size() == 1

        when: "The same bucket is requested"
        def s3Client1 = factory.getClient(s3Uri)

        then: "The same client is returned"
        1 * s3Uri.bucket() >> Optional.of(bucket4Name)
        0 * s3Uri.region()
        factory.bucketToClientKey.size() == 1
        factory.clientCache.size() == 1
        s3Client0 == s3Client1

        when: "A different bucket in the same region is requested"
        def s3Client2 = factory.getClient(s3Uri)

        then: "The same client is returned"
        1 * s3Uri.bucket() >> Optional.of(UUID.randomUUID().toString())
        1 * s3Uri.region() >> Optional.of(Region.EU_CENTRAL_1)
        factory.bucketToClientKey.size() == 2
        factory.clientCache.size() == 1
        s3Client0 == s3Client2

        when: "A bucket which needs a role is requested"
        def s3Client3 = factory.getClient(s3Uri)

        then: "A new client with assume role is created"
        1 * s3Uri.bucket() >> Optional.of(bucket2Name)
        1 * s3Uri.region() >> Optional.empty()
        factory.bucketToClientKey.size() == 3
        factory.clientCache.size() == 2
        s3Client0 != s3Client3

        when: "Same role and region but different bucket"
        def s3Client4 = factory.getClient(s3Uri)

        then: "The same client is returned"
        1 * s3Uri.bucket() >> Optional.of(bucket0Name)
        1 * s3Uri.region() >> Optional.of(Region.US_WEST_2)
        factory.bucketToClientKey.size() == 4
        factory.clientCache.size() == 2
        s3Client0 != s3Client4
        s3Client3 == s3Client4

        when: "No bucket region or role are provided"
        def s3Client5 = factory.getClient(s3Uri)

        then: "The default region is used in a new client"
        1 * s3Uri.bucket() >> Optional.of(UUID.randomUUID().toString())
        1 * s3Uri.region() >> Optional.empty()
        factory.bucketToClientKey.size() == 5
        factory.clientCache.size() == 3
        s3Client0 != s3Client5
        s3Client3 != s3Client5

        when: "Another bucket with no region or role information is requested"
        def s3Client6 = factory.getClient(s3Uri)

        then: "The same default client is returned"
        1 * s3Uri.bucket() >> Optional.of(UUID.randomUUID().toString())
        1 * s3Uri.region() >> Optional.empty()
        factory.bucketToClientKey.size() == 6
        factory.clientCache.size() == 3
        s3Client0 != s3Client6
        s3Client3 != s3Client6
        s3Client5 == s3Client6

        when: "A bucket is requested in a region that already has a client but it's a different role than before"
        def s3Client7 = factory.getClient(s3Uri)

        then: "A new client is returned"
        1 * s3Uri.bucket() >> Optional.of(bucket3Name)
        1 * s3Uri.region() >> Optional.empty()
        factory.bucketToClientKey.size() == 7
        factory.clientCache.size() == 4
        s3Client3 != s3Client7
    }

    def "Can get S3Uri from URI"() {
        def environment = new MockEnvironment()
        def credentialsProvider = Mock(AwsCredentialsProvider)
        def regionProvider = Mock(AwsRegionProvider)
        def uri = new URI("s3://bucket-name/key/path")

        when:
        def factory = new S3ClientFactory(credentialsProvider, regionProvider, environment)
        def s3Uri = factory.getS3Uri(uri)

        then:
        1 * regionProvider.getRegion() >> Region.US_EAST_1
        s3Uri != null
        s3Uri.bucket().isPresent()
        s3Uri.bucket().get() == "bucket-name"
        s3Uri.key().isPresent()
        s3Uri.key().get() == "key/path"
    }
}
