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

import io.awspring.cloud.s3.S3ProtocolResolver as SpringS3ProtocolResolver
import org.springframework.context.support.AbstractApplicationContext
import org.springframework.core.io.ProtocolResolver
import software.amazon.awssdk.services.s3.S3Client
import spock.lang.Specification

/**
 * Specifications for {@link S3ProtocolResolverRegistrar}.
 *
 * @author tgianos
 */
class S3ProtocolResolverRegistrarSpec extends Specification {

    def "Can add resolver to empty set of resolvers"() {
        def context = Mock(AbstractApplicationContext)
        def resolver = Mock(S3ProtocolResolver)
        def resolverRegistrar = new S3ProtocolResolverRegistrar(resolver)

        when: "Empty context is passed in"
        resolverRegistrar.setApplicationContext(context)

        then: "The S3 protocol resolver is added"
        1 * context.addProtocolResolver(resolver)
        1 * context.getProtocolResolvers() >> new LinkedHashSet<>()
    }

    def "Spring S3ProtocolResolver is removed if is present"() {
        def resolvers = new LinkedHashSet<ProtocolResolver>()
        def context = Mock(AbstractApplicationContext)
        def resolver = Mock(S3ProtocolResolver)
        def configurer = new S3ProtocolResolverRegistrar(resolver)
        def s3Client = Mock(S3Client)
        def springS3ProtocolResolver = new SpringS3ProtocolResolver(s3Client)
        resolvers.add(springS3ProtocolResolver)

        when: "Context with a Spring S3ProtocolResolver already present is passed in"
        configurer.setApplicationContext(context)

        then: "The S3ProtocolResolver is added and the Spring S3ProtocolResolver is removed"
        1 * context.addProtocolResolver(resolver)
        1 * context.getProtocolResolvers() >> resolvers
        !resolvers.contains(springS3ProtocolResolver)
    }
}
