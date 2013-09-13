/*
 * Copyright 2013 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package com.netflix.genie.server.resources;

import com.google.inject.Inject;
import com.netflix.genie.common.messages.HiveConfigResponse;
import com.netflix.genie.server.utils.Deployments;
import com.netflix.kayron.server.test.RunInKaryon;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link HiveConfigResourceV0} class.
 *
 * @author Jakub Narloch (jmnarloch@gmail.com)
 */
@RunWith(Arquillian.class)
@RunInKaryon(applicationId = "genie")
public class HiveConfigResourceV0TestCase {

    /**
     * Creates the test deployment.
     *
     * @return the test deployment
     */
    @SuppressWarnings("rawtypes")
    @Deployment
    public static Archive createTestArchive() {

        return Deployments.createDeployment();
    }

    /**
     * The injected {@link HiveConfigResourceV0} class.
     */
    @Inject
    private HiveConfigResourceV0 resource;

    /**
     * Test the {@link HiveConfigResourceV0#getHiveConfig(String, String, String)} method.
     */
    @Test
    public void shouldRetrieveConfigs() {

        // when
        Response response = resource.getHiveConfig(null, null, null);

        // then
        assertNotNull("The response entity was null.", response);
        assertNotNull("The response entity was null.", response.getEntity());
        assertTrue("The response entity had incorrect type.", response.getEntity() instanceof HiveConfigResponse);
        assertNull("The response list was expected to be null.",
                ((HiveConfigResponse) response.getEntity()).getHiveConfigs());
    }
}
