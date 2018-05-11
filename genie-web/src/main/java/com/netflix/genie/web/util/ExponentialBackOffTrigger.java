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

package com.netflix.genie.web.util;

import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Date;

/**
 * Trigger implementation whose scheduling delay grows exponentially based on a given factor.
 * The delay is bound between some minimum and maximum.
 * The delay can be relative to the previous task scheduling/execution/completion.
 *
 * @author mprimi
 * @since 3.3.9
 */
@ThreadSafe
public class ExponentialBackOffTrigger implements Trigger {

    private static final float POSITIVE_NUMBER = 1;
    private final DelayType delayType;
    private final long minDelay;
    private final long maxDelay;
    private final float factor;
    private long currentDelay;

    /**
     * Constructor.
     * @param delayType type of delay
     * @param minDelay minimum delay in milliseconds
     * @param maxDelay maximum delay in milliseconds
     * @param factor multiplier factor to grow the delay
     * @throws IllegalArgumentException if the minimum delay is smaller than 1, the max delay is smaller than the
     * minimum, or the factor is not positive.
     */
    public ExponentialBackOffTrigger(
        final DelayType delayType,
        final long minDelay,
        final long maxDelay,
        final float factor
    ) {
        this.delayType = delayType;
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.factor = factor;

        if (Math.signum(minDelay) != POSITIVE_NUMBER) {
            throw new IllegalArgumentException("Minimum delay must be a positive number");
        }

        if (maxDelay < minDelay) {
            throw new IllegalArgumentException("Maximum delay must be larger than minimum");
        }

        if (Math.signum(factor) != POSITIVE_NUMBER) {
            throw new IllegalArgumentException("Factor must be a positive number");
        }

        reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date nextExecutionTime(final TriggerContext triggerContext) {
        Date baseTimeOffset = null;

        switch (delayType) {
            case FROM_PREVIOUS_SCHEDULING:
                baseTimeOffset = triggerContext.lastScheduledExecutionTime();
                break;
            case FROM_PREVIOUS_EXECUTION_BEGIN:
                baseTimeOffset = triggerContext.lastActualExecutionTime();
                break;
            case FROM_PREVIOUS_EXECUTION_COMPLETION:
                baseTimeOffset = triggerContext.lastCompletionTime();
                break;
            default:
                throw new RuntimeException("Unhandled delay type: " + delayType);
        }

        if (baseTimeOffset == null) {
            baseTimeOffset = new Date();
        }

        return new Date(baseTimeOffset.toInstant().toEpochMilli() + getAndIncrementDelay());
    }

    /**
     * Reset the delay to the minimum given at construction time.
     * Example usage: if the trigger is used to slow down attempt to contact a remote service in case of error, then
     * a successful request can invoke reset, ensuring the next attempt will not be delayed.
     */
    public synchronized void reset() {
        currentDelay = minDelay;
    }

    private synchronized long getAndIncrementDelay() {
        final long delay = currentDelay;
        currentDelay = Math.min(
            maxDelay,
            (long) (factor * currentDelay)
        );
        return delay;
    }

    /**
     * How the delay is calculated.
     */
    public enum DelayType {
        /**
         * Calculate delay from the previous time the task was scheduled.
         */
        FROM_PREVIOUS_SCHEDULING,

        /**
         * Calculate delay from the start of the previous execution.
         */
        FROM_PREVIOUS_EXECUTION_BEGIN,

        /**
         * Calculate delay from the completion of the previous execution.
         */
        FROM_PREVIOUS_EXECUTION_COMPLETION,
    }
}
