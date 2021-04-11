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
package com.netflix.genie.common.internal.tracing.brave;

import spock.lang.Specification
import zipkin2.reporter.AsyncReporter;

/**
 * Specifications for {@link BraveTracingCleanup}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class BraveTracingCleanupSpec extends Specification {

    def "can flush all async reporters"() {
        def reporter0 = Mock(AsyncReporter)
        def reporter1 = Mock(AsyncReporter)
        def reporters = [reporter0, reporter1] as Set
        def cleanup = new BraveTracingCleanup(reporters)

        when:
        cleanup.cleanup()

        then:
        1 * reporter0.flush()
        1 * reporter1.flush()
    }
}
