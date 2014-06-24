/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mapper for CloudServiceExceptions.
 *
 * @author tgianos
 */
@Provider
@Named
public class CloudServiceExceptionMapper implements ExceptionMapper<CloudServiceException> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudServiceExceptionMapper.class);
    private final GenieNodeStatistics genieNodeStatistics;
    
    /**
     * Public constructor.
     * 
     * @param genieNodeStatistics The statistics object to use
     */
    @Inject
    public CloudServiceExceptionMapper(final GenieNodeStatistics genieNodeStatistics) {
        this.genieNodeStatistics = genieNodeStatistics;
    }

    /**
     * Create a response object from the exception.
     *
     * @param cse The exception to create the HTTP response from
     * @return a Response object
     */
    @Override
    public Response toResponse(final CloudServiceException cse) {
        final int code = cse.getErrorCode();
        final String errorMessage = cse.getLocalizedMessage();
        if (code >= 400 && code < 500) {
            this.genieNodeStatistics.incrGenie4xxCount();
        } else { // 5xx codes
            this.genieNodeStatistics.incrGenie5xxCount();
        }
        LOG.error("Error code: " + code + " Error Message: " + errorMessage, cse);
        return Response.status(code).entity(errorMessage).type(MediaType.TEXT_PLAIN).build();
    }
}
