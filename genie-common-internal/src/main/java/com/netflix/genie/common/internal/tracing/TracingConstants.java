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
package com.netflix.genie.common.internal.tracing;

/**
 * Constants used for adding metadata to tracing spans.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class TracingConstants {

    /**
     * The root of all tags related to Genie on spans.
     */
    public static final String GLOBAL_TAG_BASE = "genie";

    /**
     * The root for all tags related to spans occurring in the Genie agent.
     */
    public static final String AGENT_TAG_BASE = GLOBAL_TAG_BASE + ".agent";

    /**
     * The root for all tags related to spans occurring in the Genie server.
     */
    public static final String SERVER_TAG_BASE = GLOBAL_TAG_BASE + ".server";

    /**
     * The root of all tags related to spans operating on a genie job.
     */
    public static final String JOB_TAG_BASE = GLOBAL_TAG_BASE + ".job";

    /**
     * The tag for the unique job id.
     */
    public static final String JOB_ID_TAG = JOB_TAG_BASE + ".id";

    /**
     * The tag for the job name.
     */
    public static final String JOB_NAME_TAG = JOB_TAG_BASE + ".name";

    private TracingConstants() {
    }
}
