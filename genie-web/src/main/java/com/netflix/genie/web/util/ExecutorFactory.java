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

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;

/**
 * A factory for {@link org.apache.commons.exec.Executor} instances.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class ExecutorFactory {

    /**
     * Create a new {@link Executor} implementation instance.
     *
     * @param detached Whether the streams for processes run on this executor should be detached (ignored) or not
     * @return A {@link Executor} instance
     */
    public Executor newInstance(final boolean detached) {
        final Executor executor = new DefaultExecutor();
        if (detached) {
            executor.setStreamHandler(new PumpStreamHandler(null, null));
        }
        return executor;
    }
}
