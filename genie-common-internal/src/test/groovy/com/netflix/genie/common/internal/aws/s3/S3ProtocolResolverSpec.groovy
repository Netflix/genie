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
import org.springframework.cloud.aws.core.io.s3.SimpleStorageResource
import org.springframework.core.io.ResourceLoader
import org.springframework.core.task.TaskExecutor
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link S3ProtocolResolver}.
 *
 * @author tgianos
 */
class S3ProtocolResolverSpec extends Specification {

    @Shared
    def successClosure = { result -> result != null && result instanceof SimpleStorageResource }

    @Shared
    def failedClosure = { result -> result == null }

    @Unroll
    def "#can resolve #location"() {
        def s3TaskExecutor = Mock(TaskExecutor)
        def s3Client = Mock(AmazonS3)
        def s3ClientFactory = Mock(S3ClientFactory) {
            getClient(_ as AmazonS3URI) >> s3Client
        }
        def resourceLoader = Mock(ResourceLoader)
        def s3ProtocolResolver = new S3ProtocolResolver(s3ClientFactory, s3TaskExecutor)

        when:
        def resource = s3ProtocolResolver.resolve(location, resourceLoader)

        then:
        resultClosure.call(resource)

        where:
        location                                               | can     | resultClosure
        "s3://aBucket/key/path/file.tar.gz"                    | "can"   | successClosure
        "file:/tmp/blah.txt"                                   | "can't" | failedClosure
        "s3n://aBucket/blah.txt"                               | "can't" | failedClosure
        "http://s3-eu-west-1.amazonaws.com/mybucket/puppy.jpg" | "can"   | successClosure
        "http://mybucket.s3.amazonaws.com/puppy.jpg"           | "can"   | successClosure
    }
}
