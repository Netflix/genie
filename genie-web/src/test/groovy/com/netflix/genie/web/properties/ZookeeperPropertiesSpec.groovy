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
package com.netflix.genie.web.properties

import spock.lang.Specification

class ZookeeperPropertiesSpec extends Specification {

    def "ZookeeperPropertiesSpec defaults, getters and setters for "() {
        when:
        ZookeeperProperties zkProperties = new ZookeeperProperties()

        then:
        zkProperties.getLeaderPath() == "/genie/leader/"
        zkProperties.getDiscoveryPath() == "/genie/agents/"

        when:
        zkProperties.setLeaderPath("/genie/my-cluster/leader/")
        zkProperties.setDiscoveryPath("/genie/my-cluster/agents/")

        then:
        zkProperties.getLeaderPath() == "/genie/my-cluster/leader/"
        zkProperties.getDiscoveryPath() == "/genie/my-cluster/agents/"
    }
}
