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

import java.net.HttpURLConnection;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Basic tests for the JobManagerFactory.
 *
 * @author skrishnan
 */
public class TestJobManagerFactory {

    /**
     * Tests whether an invalid job type throws an exception.
     */
    @Test
    public void testInvalidJobType() {
        try {
            JobManagerFactory.getJobManager("NotSupported");
        } catch (CloudServiceException cse) {
            Assert.assertEquals(cse.getErrorCode(),
                    HttpURLConnection.HTTP_BAD_REQUEST);
        }
    }
}
