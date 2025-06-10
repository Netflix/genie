/*
 *
 *  Copyright 2023 Netflix, Inc.
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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Uri
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.transfer.s3.S3TransferManager
import spock.lang.Specification

/**
 * Specifications for {@link S3TransferManagerFactory}.
 */
class S3TransferManagerFactorySpec extends Specification {

    def "Can construct with S3ClientFactory"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getBucketProperties() >> [:]
        }

        when:
        def factory = new S3TransferManagerFactory(s3ClientFactory)

        then:
        factory != null
    }

    def "Can get async client for S3Uri"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getBucketProperties() >> [:]
        }
        def s3Uri = Mock(S3Uri)
        def clientKey = Mock(S3ClientFactory.S3ClientKey) {
            getRegion() >> Region.US_EAST_1
            getRoleARN() >> Optional.empty()
        }
        def credentialsProvider = Mock(AwsCredentialsProvider)
        def factory = new S3TransferManagerFactory(s3ClientFactory)

        when:
        def asyncClient = factory.getAsyncClient(s3Uri)

        then:
        1 * s3ClientFactory.getS3ClientKey(s3Uri) >> clientKey
        1 * s3ClientFactory.getAwsCredentialsProvider() >> credentialsProvider
        asyncClient != null
        asyncClient instanceof S3AsyncClient

        when:
        def asyncClient2 = factory.getAsyncClient(s3Uri)

        then:
        1 * s3ClientFactory.getS3ClientKey(s3Uri) >> clientKey
        0 * s3ClientFactory.getAwsCredentialsProvider()
        asyncClient2 == asyncClient // Should return cached client
    }

    def "Can get async client with role ARN"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getBucketProperties() >> [:]
        }
        def s3Uri = Mock(S3Uri)
        def clientKey = Mock(S3ClientFactory.S3ClientKey) {
            getRegion() >> Region.US_EAST_1
            getRoleARN() >> Optional.of("arn:aws:iam::123456789012:role/test-role")
        }
        def stsClient = Mock(StsClient)
        def factory = new S3TransferManagerFactory(s3ClientFactory)

        when:
        def asyncClient = factory.getAsyncClient(s3Uri)

        then:
        1 * s3ClientFactory.getS3ClientKey(s3Uri) >> clientKey
        1 * s3ClientFactory.getStsClient() >> stsClient
        asyncClient != null
        asyncClient instanceof S3AsyncClient
    }

    def "Can get transfer manager for S3Uri"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getBucketProperties() >> [:]
        }
        def s3Uri = Mock(S3Uri)
        def clientKey = Mock(S3ClientFactory.S3ClientKey) {
            getRegion() >> Region.US_EAST_1
            getRoleARN() >> Optional.empty()
        }
        def credentialsProvider = Mock(AwsCredentialsProvider)
        def factory = Spy(S3TransferManagerFactory, constructorArgs: [s3ClientFactory]) {
            getAsyncClient(s3Uri) >> Mock(S3AsyncClient)
        }

        when:
        def transferManager = factory.getTransferManager(s3Uri)

        then:
        transferManager != null
        transferManager instanceof S3TransferManager

        when:
        def transferManager2 = factory.getTransferManager(s3Uri)

        then:
        transferManager2 == transferManager // Should return cached transfer manager
    }

    def "Can get S3Uri from URI"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getBucketProperties() >> [:]
        }
        def uri = new URI("s3://bucket/key")
        def s3Uri = Mock(S3Uri)
        def factory = new S3TransferManagerFactory(s3ClientFactory)

        when:
        def result = factory.getS3Uri(uri)

        then:
        1 * s3ClientFactory.getS3Uri(uri) >> s3Uri
        result == s3Uri
    }

    def "Can get S3Uri from String"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getBucketProperties() >> [:]
        }
        def uriString = "s3://bucket/key"
        def s3Uri = Mock(S3Uri)
        def factory = new S3TransferManagerFactory(s3ClientFactory)

        when:
        def result = factory.getS3Uri(uriString)

        then:
        1 * s3ClientFactory.getS3Uri(uriString) >> s3Uri
        result == s3Uri
    }
}
