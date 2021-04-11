/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.common.internal.tracing.brave.impl

import brave.SpanCustomizer
import spock.lang.Specification

/**
 * Specifications for {@link DefaultBraveTagAdapterImpl}.
 *
 * @author tgianos
 */
class DefaultBraveTagAdapterImplSpec extends Specification {

    def "adapt proxies call"() {
        def adapter = new DefaultBraveTagAdapterImpl()
        def customizer = Mock(SpanCustomizer)
        def key = UUID.randomUUID().toString()
        def value = UUID.randomUUID().toString()

        when:
        adapter.tag(customizer, key, value)

        then:
        1 * customizer.tag(key, value) >> customizer
    }
}
