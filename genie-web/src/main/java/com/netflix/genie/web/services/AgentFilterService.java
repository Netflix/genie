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

package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.validation.Valid;

/**
 * Service to identify agent/clients that the server wants to refuse service to.
 * To be used, for example, to blacklist clients running a given version that is known to be deprecated or otherwise
 * incompatible.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentFilterService {

    /**
     * Decide whether or not a given client is welcome to interact with the server, based on its metadata.
     *
     * @param agentClientMetadata the agent client metadata
     * @throws GenieAgentRejectedException if the agent failed inspection and was rejected
     */
    void acceptOrThrow(@Valid AgentClientMetadata agentClientMetadata) throws GenieAgentRejectedException;


    /**
     * Component that inspects an Agent client metadata and makes decision on whether it is allowed to proceed.
     */
    interface AgentMetadataInspector {
        /**
         * Perform inspection of an Agent client metadata.
         *
         * @param agentClientMetadata the agent client metadata
         * @return the inspection outcome
         */
        InspectionReport inspect(@Valid AgentClientMetadata agentClientMetadata);
    }

    /**
     * Representation of the outcome of an inspection performed by an {@link AgentMetadataInspector}.
     */
    @AllArgsConstructor
    @Getter
    class InspectionReport {
        private final InspectionDecision decision;
        private final String message;

        /**
         * The possible decisions of an {@link AgentMetadataInspector}.
         */
        public enum InspectionDecision {
            /**
             * Allow this agent to proceed without further inspections.
             */
            ACCEPT,
            /**
             * Prevent this agent from proceeding without further inspections.
             */
            REJECT,
            /**
             * Allow this agent to move on to the next inspection, if any.
             */
            CONTINUE,
        }
    }
}
