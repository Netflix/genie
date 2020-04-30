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
package com.netflix.genie.agent

import org.apache.commons.lang3.StringUtils
import spock.lang.Specification

class AgentMetadataImplSpec extends Specification {
    def "Construct without arguments"() {
        when:
        AgentMetadata agentMetadata = new AgentMetadataImpl()

        then:
        !StringUtils.isBlank(agentMetadata.getAgentVersion())
        !StringUtils.isBlank(agentMetadata.getAgentHostName())
        !StringUtils.isBlank(agentMetadata.getAgentPid())
    }

    def "Construct with hostname"() {
        setup:
        String hostname = UUID.randomUUID().toString()

        when:
        AgentMetadata agentMetadata = new AgentMetadataImpl(hostname)

        then:
        !StringUtils.isBlank(agentMetadata.getAgentVersion())
        agentMetadata.getAgentHostName() == hostname
        !StringUtils.isBlank(agentMetadata.getAgentPid())
    }
}
