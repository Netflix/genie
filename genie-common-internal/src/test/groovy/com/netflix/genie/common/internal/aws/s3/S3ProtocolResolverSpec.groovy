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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.GetObjectMetadataRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.lang3.tuple.Pair
import org.springframework.core.io.ResourceLoader
import org.springframework.core.task.TaskExecutor
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link S3ProtocolResolver}.
 *
 * @author tgianos
 */
class S3ProtocolResolverSpec extends Specification {

    @Unroll
    def "can resolve #location"() {
        def s3TaskExecutor = Mock(TaskExecutor)
        def s3ObjectMetadata = Mock(ObjectMetadata) {
            getContentLength() >> 100
        }
        def s3Client = Mock(AmazonS3) {
            getObjectMetadata(_) >> s3ObjectMetadata
        }
        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as AmazonS3URI) >> s3Client
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)

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
        def s3TaskExecutor = Mock(TaskExecutor)
        def s3ObjectMetadata = Mock(ObjectMetadata) {
            getContentLength() >> 100
        }
        def s3Client = Mock(AmazonS3) {
            getObjectMetadata(_) >> s3ObjectMetadata
        }
        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as AmazonS3URI) >> s3Client
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)

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
        def s3TaskExecutor = Mock(TaskExecutor)
        def resourceLoader = Mock(ResourceLoader)
        def s3Client = Mock(AmazonS3)
        def s3ClientFactory = Mock(S3ClientFactory)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)
        def s3ObjectMetadata = Mock(ObjectMetadata)
        GetObjectMetadataRequest requestCapture

        when:
        def resource = s3ProtocolResolver.resolve(location, resourceLoader)

        then:
        1 * s3ClientFactory.getClient(_ as AmazonS3URI) >> s3Client
        1 * s3Client.getObjectMetadata(_ as GetObjectMetadataRequest) >> {
            args ->
                requestCapture = args[0] as GetObjectMetadataRequest
                return s3ObjectMetadata
        }
        1 * s3ObjectMetadata.getContentLength() >> 100
        requestCapture != null
        requestCapture.getBucketName() == "aBucket"
        requestCapture.getKey() == "key/path/file.tar.gz"
        resource != null

        where:
        location                                        | _
        "s3://aBucket/key/path/file.tar.gz#bytes=10-20" | _
        "s3://aBucket/key/path/file.tar.gz#bytes=-20"   | _
        "s3://aBucket/key/path/file.tar.gz#bytes=10-"   | _
        "s3://aBucket/key/path/file.tar.gz#"            | _
        "s3://aBucket/key/path/file.tar.gz"             | _
    }


    def "can handle resource not existing in S3"() {
        def exception = new AmazonS3Exception("...")
        exception.setStatusCode(404)

        def s3TaskExecutor = Mock(TaskExecutor)
        def s3Client = Mock(AmazonS3) {
            getObjectMetadata(_) >> { throw exception }
        }
        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as AmazonS3URI) >> s3Client
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)

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
        def exception = new AmazonS3Exception("...")
        exception.setStatusCode(406)

        def s3TaskExecutor = Mock(TaskExecutor)
        def s3Client = Mock(AmazonS3) {
            getObjectMetadata(_) >> { throw exception }
        }
        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as AmazonS3URI) >> s3Client
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)

        when:
        def resource = s3ProtocolResolver.resolve("s3://aBucket/key/path/file.tar.gz", resourceLoader)

        then:
        resource == null
        thrown(AmazonS3Exception)
    }

    def "can handle other runtime exception"() {
        def exception = new RuntimeException()

        def s3TaskExecutor = Mock(TaskExecutor)
        def s3Client = Mock(AmazonS3) {
            getObjectMetadata(_) >> { throw exception }
        }
        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as AmazonS3URI) >> s3Client
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)

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
