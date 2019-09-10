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
package com.netflix.genie.agent.execution.process;

import com.netflix.genie.common.dto.JobStatus;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A DTO POJO to capture final information about the job process this agent process was responsible for.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@SuppressWarnings("FinalClass")
public class JobProcessResult {

    private final JobStatus finalStatus;
    private final String finalStatusMessage;
    private final long stdOutSize;
    private final long stdErrSize;
    private final int exitCode;

    private JobProcessResult(final Builder builder) {
        this.finalStatus = builder.bFinalStatus;
        this.finalStatusMessage = builder.bFinalStatusMessage;
        this.stdOutSize = builder.bStdOutSize;
        this.stdErrSize = builder.bStdErrSize;
        this.exitCode = builder.bExitCode;
    }

    /**
     * A builder to create valid, immutable {@link JobProcessResult} instances.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private final JobStatus bFinalStatus;
        private final String bFinalStatusMessage;
        private final int bExitCode;
        private long bStdOutSize;
        private long bStdErrSize;

        /**
         * Constructor.
         *
         * @param finalStatus        The final {@link JobStatus} for the job. {@link JobStatus#isFinished()} must return
         *                           true
         * @param finalStatusMessage The final human readable message for the job status
         * @param exitCode           The process exit code
         * @throws IllegalArgumentException When {@literal finalStatus} is not a final status
         */
        public Builder(
            final JobStatus finalStatus,
            final String finalStatusMessage,
            final int exitCode
        ) throws IllegalArgumentException {
            if (!finalStatus.isFinished()) {
                throw new IllegalArgumentException(
                    "finalStatus must be one of the final states: "
                        + JobStatus.getFinishedStatuses()
                        + ". Was "
                        + finalStatusMessage
                );
            }
            this.bFinalStatus = finalStatus;
            this.bFinalStatusMessage = finalStatusMessage;
            this.bExitCode = exitCode;
        }

        /**
         * Set the length of the std out file in bytes if there was one.
         *
         * @param stdOutSize The length of the std out file in bytes
         * @return This builder object
         */
        public Builder withStdOutSize(final long stdOutSize) {
            this.bStdOutSize = Math.max(stdOutSize, 0L);
            return this;
        }

        /**
         * Set the length of the std error file in bytes if there was one.
         *
         * @param stdErrSize The length of the std error file in bytes
         * @return This builder object
         */
        public Builder withStdErrSize(final long stdErrSize) {
            this.bStdErrSize = Math.max(stdErrSize, 0L);
            return this;
        }

        /**
         * Create a new immutable {@link JobProcessResult} instance based on the current contents of this builder.
         *
         * @return A {@link JobProcessResult} instance
         */
        public JobProcessResult build() {
            return new JobProcessResult(this);
        }
    }
}
