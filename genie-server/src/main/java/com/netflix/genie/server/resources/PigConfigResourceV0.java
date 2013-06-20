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

package com.netflix.genie.server.resources;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.PigConfigRequest;
import com.netflix.genie.common.messages.PigConfigResponse;
import com.netflix.genie.common.model.PigConfigElement;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.services.PigConfigService;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Code for PigConfigResource - REST end-point for supporting Pig configs.
 * <p>
 * <b>NOTE</b>: Version 0
 *
 * @author skrishnan
 */
@Path("/v0/config/pig")
@Produces({ "application/xml", "application/json" })
public class PigConfigResourceV0 {

    private static Logger logger = LoggerFactory
            .getLogger(PigConfigResourceV0.class);

    private PigConfigService pcs;

    /**
     * Custom JAXB context resolver for the pig config requests/responses.
     *
     * @author skrishnan
     */
    @Provider
    public static class PigJAXBContextResolver extends JAXBContextResolver {
        /**
         * Constructor - initialize the resolver for the types that
         * this resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public PigJAXBContextResolver() throws Exception {
            super(new Class[]{PigConfigElement.class,
                    PigConfigRequest.class,
                    PigConfigResponse.class});
        }
    }

    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     */
    public PigConfigResourceV0() throws CloudServiceException {
        pcs = ConfigServiceFactory.getPigConfigImpl();
    }

    /**
     * Gets pig configuration by id.
     *
     * @param id
     *            unique id for pig configuration to get
     * @return successful response, or one with HTTP error code
     */
    @GET
    @Path("/{id}")
    public Response getPigConfig(@PathParam("id") String id) {
        logger.info("called");
        return getPigConfig(id, null, null);
    }

    /**
     * Get pig configuration for the filter criteria.
     *
     * @param id
     *            unique id for pig config
     * @param name
     *            name of pig config
     * @param type
     *            type of config - possible values: Types.Configuration
     * @return successful response, or one with HTTP error code
     */
    @GET
    @Path("/")
    public Response getPigConfig(@QueryParam("id") String id,
            @QueryParam("name") String name, @QueryParam("type") String type) {
        logger.info("called");
        PigConfigResponse pcr = pcs.getPigConfig(id, name, type);
        return ResponseUtil.createResponse(pcr);
    }

    /**
     * Create new pig configuration.
     *
     * @param request
     *            encapsulates the pig config element to create
     * @return successful response, or one with HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({ "application/xml", "application/json" })
    public Response createPigConfig(PigConfigRequest request) {
        logger.info("called to create new pig config");
        PigConfigResponse pcr = pcs.createPigConfig(request);
        return ResponseUtil.createResponse(pcr);
    }

    /**
     * Update pig configuration.
     *
     * @param id
     *            unique id of configuration to upsert
     * @param request
     *            encapsulates the pig config element to upsert
     * @return successful response, or one with HTTP error code
     */
    @PUT
    @Path("/{id}")
    @Consumes({ "application/xml", "application/json" })
    public Response updatePigConfig(@PathParam("id") String id,
            PigConfigRequest request) {
        logger.info("called to create/update pig config");

        PigConfigElement pigConfig = request.getPigConfig();
        if (pigConfig != null) {
            // include "id" in the request
            pigConfig.setId(id);
        }

        PigConfigResponse pcr = pcs.updatePigConfig(request);
        return ResponseUtil.createResponse(pcr);
    }

    /**
     * Will return an error since no params are passed.
     *
     * @return error, since no id is provided
     */
    @DELETE
    @Path("/")
    public Response deletePigConfig() {
        logger.info("called");
        return deletePigConfig(null);
    }

    /**
     * Delete a pig configuration from database.
     *
     * @param id
     *            unique if of pig configuration to delete
     * @return successful response, or one with HTTP error code
     */
    @DELETE
    @Path("/{id}")
    public Response deletePigConfig(@PathParam("id") String id) {
        logger.info("called");
        PigConfigResponse pcr = pcs.deletePigConfig(id);
        return ResponseUtil.createResponse(pcr);
    }
}
