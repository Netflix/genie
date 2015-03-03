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
import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Basic tests for the JobManagerFactory.
 *
 * @author skrishnan
 * @author tgianos
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:genie-application-test.xml")
public class TestJobManagerFactory {

    @Inject
    private JobManagerFactory factory;

    /**
     * Tests whether an invalid class name throws an exception.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testInvalidClassName() throws GenieException {
        this.factory.getJobManager(null);
    }
}
