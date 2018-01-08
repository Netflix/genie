/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.GenieTimeoutException;
import org.apache.commons.exec.ExecuteException;

import java.io.IOException;

/**
 * Interface for implementing process checking on various operating systems.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface ProcessChecker {

    /**
     * Check the status of the process the process checker was constructed to check.
     *
     * @throws GenieTimeoutException When the process has been running longer than its configured timeout period
     * @throws ExecuteException      When the check returns a non-successful exit code
     * @throws IOException           For any other problem
     */
    void checkProcess() throws GenieTimeoutException, ExecuteException, IOException;
}
