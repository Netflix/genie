/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ClusterLoadBalancer;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.env.Environment;

/**
 * Basic tests for the JobManagerFactory.
 *
 * @author tgianos
 */
public class TestJobManagerFactory {

    /**
     * Tests whether an invalid class name throws an exception.
     *
     * @throws GenieException For any problem For any problem
     */
    @Test(expected = GenieException.class)
    public void testInvalidClassName() throws GenieException {
        final JobManagerFactory factory = new JobManagerFactory(
                Mockito.mock(ClusterConfigService.class),
                Mockito.mock(ClusterLoadBalancer.class),
                Mockito.mock(Environment.class)
        );
        factory.getJobManager(null);
    }
}
