/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.util

import org.apache.commons.exec.PumpStreamHandler
import spock.lang.Specification

/**
 * Specifications for {@link ExecutorFactory}.
 *
 * @author tgianos
 */
class ExecutorFactorySpec extends Specification {

    def "can create"() {
        def factory = new ExecutorFactory()

        when:
        def executor = factory.newInstance(false)
        def streamHandler = executor.getStreamHandler()

        then:
        streamHandler instanceof PumpStreamHandler
        ((PumpStreamHandler) streamHandler).getErr() != null
        ((PumpStreamHandler) streamHandler).getOut() != null

        when:
        def executor2 = factory.newInstance(true)
        def streamHandler2 = executor2.getStreamHandler()

        then:
        streamHandler2 instanceof PumpStreamHandler
        ((PumpStreamHandler) streamHandler2).getErr() == null
        ((PumpStreamHandler) streamHandler2).getOut() == null
        streamHandler != streamHandler2
    }
}
