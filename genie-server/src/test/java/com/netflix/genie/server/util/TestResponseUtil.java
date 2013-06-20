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

package com.netflix.genie.server.util;

import java.net.HttpURLConnection;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.JobInfoResponse;

/**
 * Test case for the ResponseUtil.
 *
 * @author skrishnan
 */
public class TestResponseUtil {

    /**
     * Test whether HTTP_OK status is returned for successful response.
     */
    @Test
    public void testSuccess() {
        JobInfoResponse ji = new JobInfoResponse();
        Response resp = ResponseUtil.createResponse(ji);
        Assert.assertEquals(resp.getStatus(), HttpURLConnection.HTTP_OK);
    }

    /**
     * Test whether an error status code is returned for a failed response.
     */
    @Test
    public void testFailure() {
        JobInfoResponse ji = new JobInfoResponse(new CloudServiceException(
                HttpURLConnection.HTTP_INTERNAL_ERROR, "This is an error"));
        Response resp = ResponseUtil.createResponse(ji);
        Assert.assertEquals(resp.getStatus(),
                HttpURLConnection.HTTP_INTERNAL_ERROR);
    }
}
