/*
 * Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.server.utils.Deployments;
import com.netflix.karyon.server.test.RunInKaryon;
import javax.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the {@link ClusterConfigResourceV1} class.
 *
 * @author Jakub Narloch (jmnarloch@gmail.com)
 */
@RunWith(Arquillian.class)
@RunInKaryon(applicationId = "genie")
//TODO: Fix this
@Ignore
public class ClusterConfigResourceV1TestCase {

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
     * The injected {@link ClusterConfigResourceV1} class.
     */
    @Inject
    private ClusterConfigResourceV1 resource;

    /**
     * Test to make sure configuration is right.
     *
     * @throws CloudServiceException
     */
    @Test(expected = CloudServiceException.class)
    public void shouldRetrieveConfigs() throws CloudServiceException {
        this.resource.getCluster(null);

//        // then
//        assertNotNull("The response entity was null.", response);
//        assertNotNull("The response entity was null.", response.getEntity());
//        assertTrue("The response entity had incorrect type.", response.getEntity() instanceof ClusterConfigResponse);
//        assertNull("The response list was expected to be null.",
//                ((ClusterConfigResponse) response.getEntity()).getClusterConfigs());
    }
}
