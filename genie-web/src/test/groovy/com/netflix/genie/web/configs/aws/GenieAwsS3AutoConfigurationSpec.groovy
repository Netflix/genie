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

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.properties.S3FileTransferProperties
import com.netflix.genie.web.services.impl.S3FileTransferImpl
import io.micrometer.core.instrument.MeterRegistry
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link GenieAwsS3AutoConfiguration} class.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
class GenieAwsS3AutoConfigurationSpec extends Specification {

    def "Can build S3 File Transfer impl"() {
        def s3ClientFactory = Mock(S3ClientFactory)
        def registry = Mock(MeterRegistry)
        def s3FileTransferProperties = new S3FileTransferProperties()
        def config = new GenieAwsS3AutoConfiguration()

        when:
        def impl = config.s3FileTransferImpl(s3ClientFactory, registry, s3FileTransferProperties)

        then:
        impl != null
        impl instanceof S3FileTransferImpl
    }
}
