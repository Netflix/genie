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
package com.netflix.genie.web.introspection

import spock.lang.Specification

/**
 * Specifications for {@link GenieWebRpcInfo}.
 *
 * @author tgianos
 */
class GenieWebRpcInfoSpec extends Specification {

    def "can create and execute POJO methods"() {
        def port = 18_809

        when:
        def info = new GenieWebRpcInfo(port)

        then:
        info.getRpcPort() == port

        when:
        def info2 = new GenieWebRpcInfo(port)
        def info3 = new GenieWebRpcInfo(port + 1)

        then:
        info == info2
        info != info3
        info.hashCode() == info2.hashCode()
        info2.hashCode() != info3.hashCode()
        info.toString() == info2.toString()
        info.toString() != info3.toString()
    }
}
