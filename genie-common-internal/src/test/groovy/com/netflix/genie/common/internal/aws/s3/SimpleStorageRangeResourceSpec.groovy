/*
 *
 *  Copyright 2020 Netflix, Inc.
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

import io.awspring.cloud.s3.InMemoryBufferingS3OutputStreamProvider
import io.awspring.cloud.s3.PropertiesS3ObjectContentTypeResolver
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.lang3.tuple.Pair
import org.springframework.core.io.Resource
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.S3Exception
import spock.lang.Specification
import spock.lang.Unroll
import java.time.Instant
import java.util.function.Consumer

class SimpleStorageRangeResourceSpec extends Specification {

    String bucket = "some-bucket"
    String key = "some/object"
    S3Client client
    HeadObjectResponse headObjectResponse
    ResponseInputStream<GetObjectResponse> objectInputStream
    byte[] data = new byte[1024]
    Pair<Integer, Integer> nullRange = ImmutablePair.of(null, null)
    InMemoryBufferingS3OutputStreamProvider outputStreamProvider

    void setup() {
        this.client = Mock(S3Client)
        this.headObjectResponse = HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.now())
            .build() as HeadObjectResponse
        this.objectInputStream = Mock(ResponseInputStream)
        this.outputStreamProvider = new InMemoryBufferingS3OutputStreamProvider(client, new PropertiesS3ObjectContentTypeResolver())

        new Random().nextBytes(data)
    }

    @Unroll
    def "Read entire object (range: #range)"() {
        setup:
        byte[] buffer = new byte[512]
        GetObjectRequest capturedRequest

        when:
        SimpleStorageRangeResource resource = new SimpleStorageRangeResource(bucket, key, client, outputStreamProvider, range as Pair<Integer, Integer>)
        InputStream inputStream = resource.getInputStream()

        then:
        // Allow headObject to be called 1-2 times
        (1..2) * client.headObject(_ as Consumer) >> { Consumer consumer ->
            def builder = HeadObjectRequest.builder()
            consumer.accept(builder)
            def request = builder.build()
            assert request.bucket() == bucket
            assert request.key() == key
            return headObjectResponse
        }

        // Exactly one call to getObject with specific parameters
        1 * client.getObject(_ as GetObjectRequest) >> { GetObjectRequest request ->
            capturedRequest = request
            return objectInputStream
        }

        capturedRequest != null
        capturedRequest.bucket() == bucket
        capturedRequest.key() == key
        capturedRequest.range() == "bytes=0-99"
        resource != null
        inputStream != null

        when:
        inputStream.read(buffer, 0, buffer.size())

        then:
        1 * objectInputStream.read(buffer, 0, buffer.size()) >> 100

        when:
        inputStream.close()

        then:
        1 * objectInputStream.close()

        where:
        range                        | _
        ImmutablePair.of(null, null) | _
        ImmutablePair.of(0, 99)      | _
        ImmutablePair.of(null, 999)  | _
    }

    @Unroll
    def "Read object with range #rangeHeader (using skip()? #useSkip)"() {
        setup:
        byte[] buffer = new byte[512]
        long bytesRead
        int contentLength = 100
        assert skippedBytes + readBytes == contentLength
        ImmutablePair<Integer, Integer> range = ImmutablePair.of(rangeStart, rangeEnd)
        GetObjectRequest capturedRequest

        when:
        SimpleStorageRangeResource resource = new SimpleStorageRangeResource(bucket, key, client, outputStreamProvider, range as Pair<Integer, Integer>)
        InputStream inputStream = resource.getInputStream()

        then:
        // Allow headObject to be called 1-2 times
        (1..2) * client.headObject(_ as Consumer) >> { Consumer consumer ->
            def builder = HeadObjectRequest.builder()
            consumer.accept(builder)
            def request = builder.build()
            assert request.bucket() == bucket
            assert request.key() == key
            return headObjectResponse
        }

        // Exactly one call to getObject with specific parameters
        1 * client.getObject(_ as GetObjectRequest) >> { GetObjectRequest request ->
            capturedRequest = request
            return objectInputStream
        }

        capturedRequest != null
        capturedRequest.bucket() == bucket
        capturedRequest.key() == key
        capturedRequest.range() == "bytes=${requestedRangeStart}-${requestedRangeEnd}"
        resource != null
        inputStream != null

        when:
        if (useSkip) {
            bytesRead = inputStream.read(buffer, 0, buffer.size())
        } else {
            bytesRead = inputStream.skip(requestedRangeStart)
        }

        then:
        bytesRead == skippedBytes
        0 * objectInputStream.read(_, _, _)

        when:
        bytesRead = inputStream.read(buffer, 0, buffer.size())

        then:
        bytesRead == readBytes
        1 * objectInputStream.read(buffer, 0, buffer.size()) >> (requestedRangeEnd - requestedRangeStart) + 1

        when:
        inputStream.close()

        then:
        1 * objectInputStream.close()

        where:
        rangeHeader    | rangeStart | rangeEnd | requestedRangeStart | requestedRangeEnd | skippedBytes | readBytes | useSkip
        "bytes=1-99"   | 1          | 99       | 1                   | 99                | 1            | 99        | true
        "bytes=-10"    | null       | 10       | 90                  | 99                | 90           | 10        | true
        "bytes=50-"    | 50         | null     | 50                  | 99                | 50           | 50        | true
        "bytes=99-"    | 99         | null     | 99                  | 99                | 99           | 1         | true
        "bytes=50-999" | 50         | 999      | 50                  | 99                | 50           | 50        | true
        "bytes=1-99"   | 1          | 99       | 1                   | 99                | 1            | 99        | false
        "bytes=-10"    | null       | 10       | 90                  | 99                | 90           | 10        | false
        "bytes=50-"    | 50         | null     | 50                  | 99                | 50           | 50        | false
        "bytes=99-"    | 99         | null     | 99                  | 99                | 99           | 1         | false
        "bytes=50-999" | 50         | 999      | 50                  | 99                | 50           | 50        | false
    }

    @Unroll
    def "Invalid range #range"() {
        when:
        new SimpleStorageRangeResource(bucket, key, client, outputStreamProvider, range as Pair<Integer, Integer>)

        then:
        // Exactly one call to headObject
        1 * client.headObject(_ as Consumer) >> { Consumer consumer ->
            def builder = HeadObjectRequest.builder()
            consumer.accept(builder)
            def request = builder.build()
            assert request.bucket() == bucket
            assert request.key() == key
            return headObjectResponse
        }
        thrown(IllegalArgumentException)

        where:
        range                       | _
        ImmutablePair.of(30, 20)    | _
        ImmutablePair.of(200, null) | _
    }

    @Unroll
    def "Empty range #range"() {
        byte[] buffer = new byte[512]
        Resource resource
        long bytesRead

        when:
        resource = new SimpleStorageRangeResource(bucket, key, client, outputStreamProvider, range as Pair<Integer, Integer>)

        then:
        1 * client.headObject(_ as Consumer) >> { Consumer consumer ->
            def builder = HeadObjectRequest.builder()
            consumer.accept(builder)
            def request = builder.build()
            assert request.bucket() == bucket
            assert request.key() == key
            return HeadObjectResponse.builder()
                .contentLength(contentLength)
                .lastModified(Instant.now())
                .contentType("application/octet-stream")
                .metadata(Collections.emptyMap())
                .build()
        }
        resource != null

        when:
        InputStream inputStream = resource.getInputStream()
        bytesRead = inputStream.skip(skip)

        then:
        1 * client.headObject(_ as Consumer) >> { Consumer consumer ->
            def builder = HeadObjectRequest.builder()
            consumer.accept(builder)
            def request = builder.build()
            assert request.bucket() == bucket
            assert request.key() == key
            return HeadObjectResponse.builder()
                .contentLength(contentLength)
                .lastModified(Instant.now())
                .contentType("application/octet-stream")
                .metadata(Collections.emptyMap())
                .build()
        }
        bytesRead == skip

        when:
        bytesRead = inputStream.read(buffer, 0, buffer.size())

        then:
        bytesRead == -1

        where:
        range                        | contentLength | skip
        ImmutablePair.of(100, null)  | 100           | 100
        ImmutablePair.of(null, null) | 0             | 0
    }

    @Unroll
    def "Handle metadata error #exception"() {
        when:
        new SimpleStorageRangeResource(bucket, key, client, outputStreamProvider, nullRange)

        then:
        // Exactly one call to headObject that throws an exception
        1 * client.headObject(_ as Consumer) >> { throw exception }
        thrown(Exception)

        where:
        exception                                                | _
        new FileNotFoundException("...")                         | _
        new InvalidObjectException("...")                        | _
        new IOException("...")                                   | _
        S3Exception.builder().message("...").build()             | _
    }

    @Unroll
    def "Handle non-existent object (status code: #statusCode)"() {
        when:
        def resource = new SimpleStorageRangeResource(bucket, key, client, outputStreamProvider, nullRange)
        def exists = resource.exists()

        then:
        // Exactly one call to headObject that throws NoSuchKeyException
        1 * client.headObject(_ as Consumer) >> {
            throw NoSuchKeyException.builder().statusCode(statusCode).build()
        }
        !exists

        when:
        resource.getInputStream()

        then:
        thrown(FileNotFoundException)

        where:
        statusCode | _
        404        | _
        // Not testing 301 because the client has non-trivial logic that is harder to mock, but should behave the same
    }
}
