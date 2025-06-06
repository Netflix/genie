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

import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.lang3.tuple.Pair
import org.springframework.core.io.ResourceLoader
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.S3Uri
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Instant
import java.util.function.Consumer

/**
 * Specifications for {@link S3ProtocolResolver}.
 *
 * @author tgianos
 */
class S3ProtocolResolverSpec extends Specification {

    @Unroll
    def "can resolve #location"() {
        def headObjectResponse = HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.now())
            .contentType("application/octet-stream")
            .build()

        def s3Client = Mock(S3Client) {
            headObject(_ as Consumer) >> { Consumer consumer ->
                def builder = HeadObjectRequest.builder()
                consumer.accept(builder)
                def request = builder.build()
                assert request.bucket() == "aBucket"
                assert request.key() == "key/path/file.tar.gz"
                return headObjectResponse
            }
        }

        def mockS3Uri = Mock(S3Uri) {
            bucket() >> Optional.of("aBucket")
            key() >> Optional.of("key/path/file.tar.gz")
        }

        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as S3Uri) >> s3Client
            getS3Uri(_ as URI) >> mockS3Uri
        }

        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory)

        when:
        def resource = s3ProtocolResolver.resolve(location, resourceLoader)

        then:
        resource != null
        resource instanceof SimpleStorageRangeResource

        where:
        location                                               | _
        "s3://aBucket/key/path/file.tar.gz"                    | _
        "s3n://aBucket/blah.txt"                               | _
        "s3a://aBucket/blah.txt"                               | _
        "http://s3-eu-west-1.amazonaws.com/mybucket/puppy.jpg" | _
        "http://mybucket.s3.amazonaws.com/puppy.jpg"           | _
    }

    @Unroll
    def "can't resolve #location"() {
        def s3ClientFactory = Mock(S3ClientFactory) {
            getS3Uri(_ as URI) >> { throw new IllegalArgumentException("Not a valid S3 URI") }
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory)

        when:
        def resource = s3ProtocolResolver.resolve(location, resourceLoader)

        then:
        resource == null

        where:
        location                      | _
        "file:/tmp/blah.txt"          | _
        "s3z://aBucket/blah.txt"      | _
        "http://example.com/blah.txt" | _
    }

    @Unroll
    def "can resolve #location with valid range"() {
        def headObjectResponse = HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.now())
            .contentType("application/octet-stream")
            .build()

        def s3Client = Mock(S3Client) {
            headObject(_ as Consumer) >> { Consumer consumer ->
                def builder = HeadObjectRequest.builder()
                consumer.accept(builder)
                def request = builder.build()
                assert request.bucket() == "aBucket"
                assert request.key() == "key/path/file.tar.gz"
                return headObjectResponse
            }
        }

        def mockS3Uri = Mock(S3Uri) {
            bucket() >> Optional.of("aBucket")
            key() >> Optional.of("key/path/file.tar.gz")
        }

        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as S3Uri) >> s3Client
            getS3Uri(_ as URI) >> mockS3Uri
        }

        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory)

        when:
        def resource = s3ProtocolResolver.resolve(location, resourceLoader)

        then:
        resource != null
        resource instanceof SimpleStorageRangeResource

        where:
        location                                        | _
        "s3://aBucket/key/path/file.tar.gz#bytes=10-20" | _
        "s3://aBucket/key/path/file.tar.gz#bytes=-20"   | _
        "s3://aBucket/key/path/file.tar.gz#bytes=10-"   | _
        "s3://aBucket/key/path/file.tar.gz#"            | _
        "s3://aBucket/key/path/file.tar.gz"             | _
    }

    def "can handle resource not existing in S3"() {
        def exception = NoSuchKeyException.builder().build()

        def s3Client = Mock(S3Client) {
            headObject(_ as Consumer) >> { throw exception }
        }

        def mockS3Uri = Mock(S3Uri) {
            bucket() >> Optional.of("aBucket")
            key() >> Optional.of("key/path/file.tar.gz")
        }

        def s3ClientFactory = Mock(S3ClientFactory) {
            getS3Uri(_ as URI) >> mockS3Uri
            getClient(_ as S3Uri) >> s3Client
        }

        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory)

        when:
        def resource = s3ProtocolResolver.resolve("s3://aBucket/key/path/file.tar.gz", resourceLoader)

        then:
        resource != null

        when:
        boolean exists = resource.exists()

        then:
        !exists
    }

    def "can handle resource error"() {
        def exception = S3Exception.builder().statusCode(406).build()

        def s3Client = Mock(S3Client) {
            headObject(_ as Consumer) >> { throw exception }
        }

        def mockS3Uri = Mock(S3Uri) {
            bucket() >> Optional.of("aBucket")
            key() >> Optional.of("key/path/file.tar.gz")
        }

        def s3ClientFactory = Mock(S3ClientFactory) {
            getS3Uri(_ as URI) >> mockS3Uri
            getClient(_ as S3Uri) >> s3Client
        }

        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory)

        when:
        s3ProtocolResolver.resolve("s3://aBucket/key/path/file.tar.gz", resourceLoader)

        then:
        thrown(S3Exception)
    }

    def "can handle other runtime exception"() {
        def exception = new RuntimeException("Test exception")

        def s3Client = Mock(S3Client) {
            headObject(_ as Consumer) >> { throw exception }
        }

        def mockS3Uri = Mock(S3Uri) {
            bucket() >> Optional.of("aBucket")
            key() >> Optional.of("key/path/file.tar.gz")
        }

        def s3ClientFactory = Mock(S3ClientFactory) {
            getS3Uri(_ as URI) >> mockS3Uri
            getClient(_ as S3Uri) >> s3Client
        }

        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory)

        when:
        s3ProtocolResolver.resolve("s3://aBucket/key/path/file.tar.gz", resourceLoader)

        then:
        thrown(RuntimeException)
    }

    @Unroll
    def "can parse range header: #rangeHeader"() {
        Pair<Integer, Integer> range

        when:
        range = S3ProtocolResolver.parseRangeHeader(rangeHeader)

        then:
        range == expectedRange

        where:
        rangeHeader   | expectedRange
        "bytes="      | ImmutablePair.of(null, null)
        "blah"        | ImmutablePair.of(null, null)
        ""            | ImmutablePair.of(null, null)
        null          | ImmutablePair.of(null, null)
        "bytes=-10"   | ImmutablePair.of(null, 10)
        "bytes=10-20" | ImmutablePair.of(10, 20)
        "bytes=10-"   | ImmutablePair.of(10, null)
    }
}
