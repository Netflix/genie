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
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver
import org.springframework.context.support.AbstractApplicationContext
import org.springframework.core.io.ProtocolResolver
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

    def "SimpleResourceResolver is removed if is present"() {
        def resolvers = new LinkedHashSet<ProtocolResolver>()
        def context = Mock(AbstractApplicationContext)
        def resolver = Mock(S3ProtocolResolver)
        def configurer = new S3ProtocolResolverRegistrar(resolver)
        def amazonS3 = Mock(AmazonS3)
        def simpleStorageResolver = new SimpleStorageProtocolResolver(amazonS3)
        resolvers.add(simpleStorageResolver)

        when: "Context with a SimpleStorageProtocolResolver already present is passed in"
        configurer.setApplicationContext(context)

        then: "The S3ProtocolResolver is added and the SimpleStorageProtocolResolver is removed"
        1 * context.addProtocolResolver(resolver)
        1 * context.getProtocolResolvers() >> resolvers
        !resolvers.contains(simpleStorageResolver)
    }
}
