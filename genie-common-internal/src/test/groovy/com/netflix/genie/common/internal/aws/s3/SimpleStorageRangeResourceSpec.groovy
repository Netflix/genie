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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.amazonaws.services.s3.model.GetObjectMetadataRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import org.apache.commons.lang3.tuple.ImmutablePair
import org.apache.commons.lang3.tuple.Pair
import org.springframework.core.io.Resource
import org.springframework.core.task.TaskExecutor
import spock.lang.Specification
import spock.lang.Unroll

class SimpleStorageRangeResourceSpec extends Specification {

    String bucket = "some-bucket"
    String key = "some/object"
    String version = null
    AmazonS3 client
    TaskExecutor taskExecutor
    ObjectMetadata objectMetadata
    S3Object object
    S3ObjectInputStream objectInputStream
    byte[] data = new byte[1024]
    Pair<Integer, Integer> nullRange = ImmutablePair.of(null, null)

    void setup() {
        this.client = Mock(AmazonS3)
        this.taskExecutor = Mock(TaskExecutor)
        this.objectMetadata = Mock(ObjectMetadata)
        this.object = Mock(S3Object)
        this.objectInputStream = Mock(S3ObjectInputStream)

        new Random().nextBytes(data)
    }

    @Unroll
    def "Read entire object (range: #range)"() {
        setup:
        byte[] buffer = new byte[512]
        GetObjectRequest getObjectRequestCapture

        when:
        SimpleStorageRangeResource resource = new SimpleStorageRangeResource(client, bucket, key, version, taskExecutor, range as Pair<Integer, Integer>)
        InputStream inputStream = resource.getInputStream()

        then:
        1 * client.getObjectMetadata(_ as GetObjectMetadataRequest) >> objectMetadata
        1 * objectMetadata.getContentLength() >> 100
        1 * client.getObject(_ as GetObjectRequest) >> {
            args ->
                getObjectRequestCapture = args[0] as GetObjectRequest
                return object
        }
        1 * object.getObjectContent() >> objectInputStream
        getObjectRequestCapture != null
        getObjectRequestCapture.range == [0, 99] as long[]
        resource != null
        inputStream != null

        when:
        inputStream.read(buffer, 0, buffer.size())

        then:
        1 * this.objectInputStream.read(buffer, 0, buffer.size()) >> 100

        when:
        inputStream.close()

        then:
        1 * this.objectInputStream.close()

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
        GetObjectRequest getObjectRequestCapture
        long bytesRead
        int contentLength = 100
        assert skippedBytes + readBytes == contentLength
        ImmutablePair<Integer, Integer> range = ImmutablePair.of(rangeStart, rangeEnd)

        when:
        SimpleStorageRangeResource resource = new SimpleStorageRangeResource(client, bucket, key, version, taskExecutor, range as Pair<Integer, Integer>)
        InputStream inputStream = resource.getInputStream()

        then:
        1 * client.getObjectMetadata(_ as GetObjectMetadataRequest) >> objectMetadata
        1 * objectMetadata.getContentLength() >> contentLength
        1 * client.getObject(_ as GetObjectRequest) >> {
            args ->
                getObjectRequestCapture = args[0] as GetObjectRequest
                return object
        }
        1 * object.getObjectContent() >> objectInputStream
        getObjectRequestCapture != null
        getObjectRequestCapture.range == [requestedRangeStart, requestedRangeEnd] as long[]
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
        0 * this.objectInputStream.read(_, _, _)

        when:
        bytesRead = inputStream.read(buffer, 0, buffer.size())

        then:
        bytesRead == readBytes
        1 * this.objectInputStream.read(buffer, 0, buffer.size()) >> (requestedRangeEnd - requestedRangeStart) + 1

        when:
        inputStream.close()

        then:
        1 * this.objectInputStream.close()

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
        def contentLength = 100

        when:
        new SimpleStorageRangeResource(client, bucket, key, version, taskExecutor, range as Pair<Integer, Integer>)

        then:
        1 * client.getObjectMetadata(_ as GetObjectMetadataRequest) >> objectMetadata
        1 * objectMetadata.getContentLength() >> contentLength
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
        resource = new SimpleStorageRangeResource(client, bucket, key, version, taskExecutor, range as Pair<Integer, Integer>)

        then:
        1 * client.getObjectMetadata(_ as GetObjectMetadataRequest) >> objectMetadata
        1 * objectMetadata.getContentLength() >> contentLength
        0 * client.getObject(_)
        resource != null

        when:
        InputStream inputStream = resource.getInputStream()
        bytesRead = inputStream.skip(skip)

        then:
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
        new SimpleStorageRangeResource(client, bucket, key, version, taskExecutor, nullRange)

        then:
        1 * client.getObjectMetadata(_ as GetObjectMetadataRequest) >> {
            throw exception
        }
        thrown(Exception)

        where:
        exception                         | _
        new FileNotFoundException("...")  | _
        new InvalidObjectException("...") | _
        new IOException("...")            | _
        new AmazonS3Exception("...")      | _
    }

    @Unroll
    def "Handle non-existent object (status code: #statusCode)"() {
        when:
        def resource = new SimpleStorageRangeResource(client, bucket, key, version, taskExecutor, nullRange)
        def exists = resource.exists()

        then:
        1 * client.getObjectMetadata(_ as GetObjectMetadataRequest) >> {
            def exception = new AmazonS3Exception("...")
            exception.setStatusCode(statusCode)
            throw exception
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
