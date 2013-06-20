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
import com.netflix.genie.common.messages.HiveConfigRequest;
import com.netflix.genie.common.messages.HiveConfigResponse;
import com.netflix.genie.common.model.HiveConfigElement;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.services.HiveConfigService;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Code for HiveConfigResource - REST end-point for supporting Hive configs.
 * <p>
 * <b>NOTE</b>: Version 0
 *
 * @author skrishnan
 */
@Path("/v0/config/hive")
@Produces({ "application/xml", "application/json" })
public class HiveConfigResourceV0 {

    private HiveConfigService hcs;
    private static Logger logger = LoggerFactory
            .getLogger(HiveConfigResourceV0.class);

    /**
     * Custom JAXB context resolver based for the hive config request/responses.
     *
     * @author skrishnan
     */
    @Provider
    public static class HiveJAXBContextResolver extends JAXBContextResolver {
        /**
         * Constructor - initialize the resolver for the types that
         * this resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public HiveJAXBContextResolver() throws Exception {
            super(new Class[]{HiveConfigElement.class,
                    HiveConfigRequest.class,
                    HiveConfigResponse.class});
        }
    }

    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     *             if there is any error
     */
    public HiveConfigResourceV0() throws CloudServiceException {
        hcs = ConfigServiceFactory.getHiveConfigImpl();
    }

    /**
     * Get Hive config for given id.
     *
     * @param id
     *            unique id for hive config
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/{id}")
    public Response getHiveConfig(@PathParam("id") String id) {
        logger.info("called");
        return getHiveConfig(id, null, null);
    }

    /**
     * Get Hive config based on user params.
     *
     * @param id
     *            unique id for config (optional)
     * @param name
     *            name for config (optional)
     * @param type
     *            type for config (optional)
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/")
    public Response getHiveConfig(@QueryParam("id") String id,
            @QueryParam("name") String name, @QueryParam("type") String type) {
        logger.info("called");
        HiveConfigResponse hcr = hcs.getHiveConfig(id, name, type);
        return ResponseUtil.createResponse(hcr);
    }

    /**
     * Create new hive config.
     *
     * @param request
     *            contains the hive config element for creation
     * @return successful response, or one with an HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({ "application/xml", "application/json" })
    public Response createHiveConfig(HiveConfigRequest request) {
        logger.info("called to create new hive config");
        HiveConfigResponse hcr = hcs.createHiveConfig(request);
        return ResponseUtil.createResponse(hcr);
    }

    /**
     * Insert/update hive config.
     *
     * @param id
     *            unique id for config to upsert
     * @param request
     *            contains the hive config element for update
     * @return successful response, or one with an HTTP error code
     */
    @PUT
    @Path("/{id}")
    @Consumes({ "application/xml", "application/json" })
    public Response updateHiveConfig(@PathParam("id") String id,
            HiveConfigRequest request) {
        logger.info("called to create/update hive config");
        HiveConfigElement hiveConfig = request.getHiveConfig();
        if (hiveConfig != null) {
            // include "id" in the request
            hiveConfig.setId(id);
        }

        HiveConfigResponse hcr = hcs.updateHiveConfig(request);
        return ResponseUtil.createResponse(hcr);
    }

    /**
     * Delete without an id, returns an error.
     *
     * @return error code, since no id is provided
     */
    @DELETE
    @Path("/")
    public Response deleteHiveConfig() {
        logger.info("called");
        return deleteHiveConfig(null);
    }

    /**
     * Delete a hive config from database.
     *
     * @param id
     *            unique id for config to delete
     * @return successful response, or one with an HTTP error code
     */
    @DELETE
    @Path("/{id}")
    public Response deleteHiveConfig(@PathParam("id") String id) {
        logger.info("called");
        HiveConfigResponse hcr = hcs.deleteHiveConfig(id);
        return ResponseUtil.createResponse(hcr);
    }
}
