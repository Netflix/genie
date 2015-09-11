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
package com.netflix.genie.web.startup;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobmanager.JobJanitor;
import com.netflix.genie.core.metrics.JobCountMonitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the GenieSpringBootstrap class.
 *
 * @author tgianos
 */
public class TestGenieSpringBootstrap {

    private GenieSpringBootstrap genieSpringBootstrap;

    private JobJanitor janitor;
    private JobCountMonitor monitor;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.janitor = Mockito.mock(JobJanitor.class);
        this.monitor = Mockito.mock(JobCountMonitor.class);

        this.genieSpringBootstrap = new GenieSpringBootstrap(
                this.janitor,
                this.monitor
        );
    }

    /**
     * Test that the initialize method is called after construction.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testInitialize() throws GenieException {
        this.genieSpringBootstrap.initialize();
    }

    /**
     * Test the shutdown method.
     */
    @Test
    public void testShutdown() {
        this.genieSpringBootstrap.shutdown();
        Mockito.verify(this.janitor, Mockito.times(1)).setStop(true);
        Mockito.verify(this.monitor, Mockito.times(1)).setStop(true);
    }
}
