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
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Types.ClusterStatus;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import java.util.ArrayList;
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
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for ClusterConfigResourceV1 - REST end-point for supporting cluster
 * configurations.
 *
 * @author amsharma
 * @author tgianos
 */
@Path("/v1/config/clusters")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
public class ClusterConfigResourceV1 {

    private final ClusterConfigService ccs;
    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterConfigResourceV1.class);

    /**
     * Default constructor.
     *
     * @throws CloudServiceException if there is any error
     */
    public ClusterConfigResourceV1() throws CloudServiceException {
        this.ccs = ConfigServiceFactory.getClusterConfigImpl();
    }

    /**
     * Get cluster configuration from unique id.
     *
     * @param id id for the cluster
     * @return the cluster
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}")
    public Cluster getClusterConfig(@PathParam("id") final String id) throws CloudServiceException {
        LOG.debug("called with id: " + id);
        return this.ccs.getClusterConfig(id);
    }

    /**
     * Get cluster config based on user params. If empty strings are passed for
     * they are treated as nulls (not false).
     *
     * @param name cluster name (can be a pattern)
     * @param statuses valid types - Types.ClusterStatus
     * @param tags tags for the cluster
     * @param minUpdateTime min time when cluster configuration was updated
     * @param maxUpdateTime max time when cluster configuration was updated
     * @param limit number of entries to return
     * @param page page number
     * @return the Clusters found matching the criteria
     * @throws CloudServiceException
     */
    @GET
    public List<Cluster> getClusterConfigs(
            @QueryParam("name") final String name,
            @QueryParam("status") final List<String> statuses,
            @QueryParam("tag") final List<String> tags,
            @QueryParam("minUpdateTime") final Long minUpdateTime,
            @QueryParam("maxUpdateTime") final Long maxUpdateTime,
            @QueryParam("page") @DefaultValue("0") int page,
            @QueryParam("limit") @DefaultValue("1024") int limit)
            throws CloudServiceException {
        LOG.debug("called");
        //Create this conversion internal in case someone uses lower case by accident?
        List<ClusterStatus> enumStatuses = null;
        if (statuses != null) {
            enumStatuses = new ArrayList<ClusterStatus>();
            for (final String status : statuses) {
                enumStatuses.add(ClusterStatus.parse(status));
            }
        }
        return this.ccs.getClusterConfigs(name, enumStatuses, tags, minUpdateTime, maxUpdateTime, limit, page);
    }

    /**
     * Create cluster configuration.
     *
     * @param cluster contains the cluster information to create
     * @return The created cluster
     * @throws CloudServiceException
     */
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Cluster createClusterConfig(final Cluster cluster) throws CloudServiceException {
        LOG.debug("called to create new cluster");
        return this.ccs.createClusterConfig(cluster);
    }

    /**
     * Update a cluster configuration.
     *
     * @param id unique if for cluster to update
     * @param updateCluster contains the cluster information to update
     * @return the updated cluster
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Cluster updateClusterConfig(
            @PathParam("id") final String id,
            final Cluster updateCluster) throws CloudServiceException {
        LOG.debug("called to create/update cluster");
        return this.ccs.updateClusterConfig(id, updateCluster);
    }

    /**
     * Delete a cluster configuration.
     *
     * @param id unique id for cluster to delete
     * @return the deleted cluster
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}")
    public Cluster deleteClusterConfig(@PathParam("id") final String id) throws CloudServiceException {
        LOG.debug("delete called for id: " + id);
        return this.ccs.deleteClusterConfig(id);
    }
}
