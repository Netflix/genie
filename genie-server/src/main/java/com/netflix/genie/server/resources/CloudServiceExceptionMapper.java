package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mapper for CloudServiceExceptions
 *
 * @author tgianos
 */
@Provider
public class CloudServiceExceptionMapper implements ExceptionMapper<CloudServiceException> {
    private static final Logger LOG = LoggerFactory.getLogger(CloudServiceExceptionMapper.class);
    private static final GenieNodeStatistics STATS = GenieNodeStatistics.getInstance();

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
            STATS.incrGenie4xxCount();
        } else { // 5xx codes
            STATS.incrGenie5xxCount();
        }
        LOG.error("Error code: " + code + " Error Message: " + errorMessage, cse);
        return Response.status(code).entity(errorMessage).type(MediaType.TEXT_PLAIN).build();
    }
}
