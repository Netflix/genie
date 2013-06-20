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

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.messages.BaseResponse;
import com.netflix.genie.server.metrics.GenieNodeStatistics;

/**
 * Util class to generate responses - used by all resources.
 *
 * @author skrishnan
 */
public final class ResponseUtil {
    private static Logger logger = LoggerFactory.getLogger(ResponseUtil.class);
    private static GenieNodeStatistics stats = GenieNodeStatistics
            .getInstance();

    private ResponseUtil() {
        // never called
    }

    /**
     * Construct a response with proper error code.
     *
     * @param tr
     *            BaseResponse class to marshal
     * @return JAX-RS response class with correct error code
     */
    public static Response createResponse(BaseResponse tr) {
        Response response = null;

        // return OK or error depending on the response
        if (tr.requestSucceeded()) {
            response = Response.ok(tr).build();
        } else {
            logger.error(tr.getErrorMsg());
            logger.error("Error code: " + tr.getErrorCode());
            response = Response.status(tr.getErrorCode()).entity(tr).build();
        }

        // increment counters
        int code = tr.getErrorCode();
        if (code == 200) {
            stats.incrGenie2xxCount();
        } else if ((code >= 400) && (code < 500)) {
            stats.incrGenie4xxCount();
        } else { // 5xx codes
            stats.incrGenie5xxCount();
        }

        return response;
    }
}
