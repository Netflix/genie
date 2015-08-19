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
package com.netflix.genie.server.jobmanager.impl;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.netflix.genie.server.SpringIntegrationTestBase;
import com.netflix.genie.server.jobmanager.JobJanitor;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;

/**
 * Test code for the job janitor class, which marks un-updated jobs as zombies.
 *
 * @author skrishnan
 * @author tgianos
 */
public class TestJobJanitorImpl extends SpringIntegrationTestBase {

    @Inject
    private JobJanitor janitor;

    /**
     * Test whether the janitor cleans up zombie jobs correctly.
     *
     * @throws Exception For any issue
     */
    @Test
    @DatabaseSetup("testMarkZombies.xml")
    public void testMarkZombies() throws Exception {
        Assert.assertEquals(2, this.janitor.markZombies());
    }
}
