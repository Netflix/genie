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
     * The command that was entered on the CLI for the agent to execute.
     */
    public static final String AGENT_CLI_COMMAND_NAME_TAG = AGENT_TAG_BASE + "cli.command.name";

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
     * The tag to represent that this span contains a new job submission.
     */
    public static final String NEW_JOB_TAG = JOB_TAG_BASE + ".new";

    /**
     * The tag for the job name.
     */
    public static final String JOB_NAME_TAG = JOB_TAG_BASE + ".name";

    /**
     * The tag for the job user.
     */
    public static final String JOB_USER_TAG = JOB_TAG_BASE + ".user";

    /**
     * The tag for the job command id.
     */
    public static final String JOB_CLUSTER_ID_TAG = JOB_TAG_BASE + ".cluster.id";

    /**
     * The tag for the job command id.
     */
    public static final String JOB_CLUSTER_NAME_TAG = JOB_TAG_BASE + ".cluster.name";

    /**
     * The tag for the job command id.
     */
    public static final String JOB_COMMAND_ID_TAG = JOB_TAG_BASE + ".command.id";

    /**
     * The tag for the job command id.
     */
    public static final String JOB_COMMAND_NAME_TAG = JOB_TAG_BASE + ".command.name";

    /**
     * Convenience constant for representing a flag tag with a value of {@literal true}.
     */
    public static final String TRUE_VALUE = "true";

    private TracingConstants() {
    }
}
