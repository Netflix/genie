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

package com.netflix.genie.web.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Representation of the outcome of an inspection performed by an {@link AgentMetadataInspector}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@AllArgsConstructor
@Getter
public class InspectionReport {
    private final Decision decision;
    private final String message;

    /**
     * Factory method for {@link InspectionReport}.
     *
     * @param message a message
     * @return a new {@link InspectionReport}
     */
    public static InspectionReport newRejection(final String message) {
        return new InspectionReport(Decision.REJECT, message);
    }

    /**
     * Factory method for {@link InspectionReport}.
     *
     * @param message a message
     * @return a new {@link InspectionReport}
     */
    public static InspectionReport newAcceptance(final String message) {
        return new InspectionReport(Decision.ACCEPT, message);
    }

    /**
     * The possible outcomes of an inspection.
     */
    public enum Decision {
        /**
         * Subject passed the inspection and is allowed to proceed.
         */
        ACCEPT,
        /**
         * Subject failed the inspection and is not allowed to proceed.
         */
        REJECT;

        static Decision flip(final Decision decision) {
            switch (decision) {
                case REJECT:
                    return ACCEPT;
                case ACCEPT:
                    return REJECT;

                default:
                    throw new RuntimeException("Unexpected: " + decision.name());
            }
        }
    }

}
