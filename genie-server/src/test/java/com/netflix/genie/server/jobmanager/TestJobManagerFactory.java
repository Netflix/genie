/*
 *
 *  Copyright 2013 Netflix, Inc.
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
package com.netflix.genie.server.jobmanager;

import com.netflix.genie.common.exceptions.CloudServiceException;
import org.junit.Test;

/**
 * Basic tests for the JobManagerFactory.
 *
 * @author skrishnan
 */
public class TestJobManagerFactory {
    /**
     * Tests whether an invalid class name throws an exception.
     *
     * @throws com.netflix.genie.common.exceptions.CloudServiceException
     */
    @Test(expected = CloudServiceException.class)
    public void testInvalidClassName() throws CloudServiceException {
        final JobManagerFactory factory = new JobManagerFactory();
        factory.getJobManager(null);
    }
}
