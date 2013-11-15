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

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ClusterConfigElement;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Code for ClusterConfigResource - REST end-point for supporting Cluster
 * configs.
 * <p>
 * <b>NOTE</b>: Version 0
 *
 * @author skrishnan
 */
@Path("/v0/config/cluster")
@Produces({ "application/xml", "application/json" })
public class ClusterConfigResourceV0 {

    private ClusterConfigService ccs;
    private static Logger logger = LoggerFactory
            .getLogger(ClusterConfigResourceV0.class);

    /**
     * Custom JAXB context resolver for the cluster config requests/responses.
     *
     * @author skrishnan
     */
    @Provider
    public static class ClusterJAXBContextResolver extends JAXBContextResolver {
        /**
         * Constructor - initialize the resolver for the types that
         * this resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public ClusterJAXBContextResolver() throws Exception {
            super(new Class[]{ClusterConfigElement.class,
                    ClusterConfigRequest.class,
                    ClusterConfigResponse.class});
        }
    }

    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     *             if there is any error
     */
    public ClusterConfigResourceV0() throws CloudServiceException {
        ccs = ConfigServiceFactory.getClusterConfigImpl();
    }

    /**
     * Get cluster config from unique id.
     *
     * @param id
     *            id for cluster
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/{id}")
    public Response getClusterConfig(@PathParam("id") String id) {
        logger.info("called with id: " + id);
        ClusterConfigResponse ccr = ccs.getClusterConfig(id);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Get cluster config based on user params. If empty strings are passed for
     * prod, test, unitTest, adHoc, sla, bonus and hasStats, they are treated as
     * nulls (not false).
     *
     * @param id
     *            unique id for cluster (can be a pattern)
     * @param name
     *            cluster name (can be a pattern)
     * @param prod
     *            if cluster supports prod jobs
     * @param test
     *            if cluster supports test jobs
     * @param unitTest
     *            if cluster supports unitTest (dev) jobs
     * @param adHoc
     *            if cluster supports ad-hoc jobs
     * @param sla
     *            if cluster supports sla jobs
     * @param bonus
     *            if cluster supports bonus jobs
     * @param jobType
     *            valid job types - Types.JobType
     * @param status
     *            valid types - Types.ClusterStatus
     * @param hasStats
     *            whether the cluster is logging statistics or not
     * @param minUpdateTime
     *            min time when cluster config was updated
     * @param maxUpdateTime
     *            max time when cluster config was updated
     * @param limit
     *            number of entries to return
     * @param page
     *            page number
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/")
    public Response getClusterConfig(@QueryParam("id") String id,
            @QueryParam("name") String name, @QueryParam("prod") String prod,
            @QueryParam("test") String test,
            @QueryParam("unitTest") String unitTest,
            @QueryParam("adHoc") String adHoc, @QueryParam("sla") String sla,
            @QueryParam("bonus") String bonus,
            @QueryParam("jobType") String jobType,
            @QueryParam("status") List<String> status,
            @QueryParam("hasStats") String hasStats,
            @QueryParam("minUpdateTime") Long minUpdateTime,
            @QueryParam("maxUpdateTime") Long maxUpdateTime,
            @QueryParam("limit") @DefaultValue("1024") int limit,
            @QueryParam("page") @DefaultValue("0") int page) {
        logger.info("called");
        // treat empty string values for booleans as nulls, not false
        ClusterConfigResponse ccr = ccs
                .getClusterConfig(
                        id,
                        name,
                        (prod != null) && (!prod.isEmpty()) ? Boolean
                                .valueOf(prod) : null,
                        (test != null) && (!test.isEmpty()) ? Boolean
                                .valueOf(test) : null,
                        (unitTest != null) && (!unitTest.isEmpty()) ? Boolean
                                .valueOf(unitTest) : null,
                        (adHoc != null) && (!adHoc.isEmpty()) ? Boolean
                                .valueOf(adHoc) : null,
                        (sla != null) && (!sla.isEmpty()) ? Boolean
                                .valueOf(sla) : null,
                        (bonus != null) && (!bonus.isEmpty()) ? Boolean
                                .valueOf(bonus) : null,
                        jobType,
                        status,
                        (hasStats != null) && (!hasStats.isEmpty()) ? Boolean
                                .valueOf(hasStats) : null, minUpdateTime,
                        maxUpdateTime, limit, page);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Create cluster configuration.
     *
     * @param request
     *            contains the cluster config element for this cluster
     * @return successful response, or one with an HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({ "application/xml", "application/json" })
    public Response createClusterConfig(ClusterConfigRequest request) {
        logger.info("called to create new cluster");
        ClusterConfigResponse ccr = ccs.createClusterConfig(request);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Update/insert cluster configuration.
     *
     * @param id
     *            unique if for cluster to upsert
     * @param request
     *            contains the cluster config element for this cluster
     * @return successful response, or one with an HTTP error code
     */
    @PUT
    @Path("/{id}")
    @Consumes({ "application/xml", "application/json" })
    public Response updateClusterConfig(@PathParam("id") String id,
            ClusterConfigRequest request) {
        logger.info("called to create/update cluster");

        ClusterConfigElement clusterConfig = request.getClusterConfig();
        if (clusterConfig != null) {
            // include "id" in the request
            clusterConfig.setId(id);
        }

        ClusterConfigResponse ccr = ccs.updateClusterConfig(request);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Delete cluster configuration - this will throw an error since no params
     * are sent.
     *
     * @return return a 4XX error
     */
    @DELETE
    @Path("/")
    public Response deleteClusterConfig() {
        logger.info("called");
        // this will just return an error
        return deleteClusterConfig(null);
    }

    /**
     * Delete entry for cluster.
     *
     * @param id
     *            unique id for cluster to delete
     * @return successful response, or one with an HTTP error code
     */
    @DELETE
    @Path("/{id}")
    public Response deleteClusterConfig(@PathParam("id") String id) {
        logger.info("delete called for id: " + id);
        ClusterConfigResponse ccr = ccs.deleteClusterConfig(id);
        return ResponseUtil.createResponse(ccr);
    }
}
