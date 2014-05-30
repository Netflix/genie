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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for ApplicationConfigResource - REST end-point for supporting
 * Application.
 *
 * @author amsharma
 */
@Path("/v1/config/cluster")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
public class ClusterConfigResourceV1 {

    private final ClusterConfigService ccs;
    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterConfigResourceV1.class);

    /**
     * Custom JAXB context resolver for the cluster config requests/responses.
     *
     * @author amsharma
     */
    @Provider
    public static class ClusterJAXBContextResolver extends JAXBContextResolver {

        /**
         * Constructor - initialize the resolver for the types that this
         * resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public ClusterJAXBContextResolver() throws Exception {
            super(new Class[]{Cluster.class,
                ClusterConfigRequest.class,
                ClusterConfigResponse.class});
        }
    }

    /**
     * Default constructor.
     *
     * @throws CloudServiceException if there is any error
     */
    public ClusterConfigResourceV1() throws CloudServiceException {
        ccs = ConfigServiceFactory.getClusterConfigImpl();
    }

    /**
     * Get cluster config from unique id.
     *
     * @param id id for cluster
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/{id}")
    public Response getClusterConfig(@PathParam("id") String id) {
        LOG.info("called with id: " + id);
        ClusterConfigResponse ccr = ccs.getClusterConfig(id);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Get cluster config based on user params. If empty strings are passed for
     * they are treated as nulls (not false).
     *
     * @param id unique id for cluster (can be a pattern)
     * @param name cluster name (can be a pattern)
     * @param status valid types - Types.ClusterStatus
     * @param tags tags for the cluster
     * @param minUpdateTime min time when cluster config was updated
     * @param maxUpdateTime max time when cluster config was updated
     * @param limit number of entries to return
     * @param page page number
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/")
    public Response getClusterConfig(@QueryParam("id") String id,
            @QueryParam("name") String name,
            @QueryParam("status") List<String> status,
            @QueryParam("tags") List<String> tags,
            @QueryParam("minUpdateTime") Long minUpdateTime,
            @QueryParam("maxUpdateTime") Long maxUpdateTime,
            @QueryParam("limit") @DefaultValue("1024") int limit,
            @QueryParam("page") @DefaultValue("0") int page) {
        LOG.info("called");
        // treat empty string values for booleans as nulls, not false
        ClusterConfigResponse ccr = ccs.getClusterConfig(id, name, status, tags, minUpdateTime, maxUpdateTime, limit, page);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Create cluster configuration.
     *
     * @param request contains the cluster config element for this cluster
     * @return successful response, or one with an HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response createClusterConfig(ClusterConfigRequest request) {
        LOG.info("called to create new cluster");

        // Need to get the Cluster object and fetch the command objects from the DB
        // to set it in the object.
//        Cluster ce = request.getClusterConfig();
//        if (ce != null) {
//            ArrayList<String> cmdIds = ce.getCmdIds();
//
//            if(cmdIds != null) {
//                PersistenceManager<CommandConfigElement> pma = new PersistenceManager<CommandConfigElement>();
//                ArrayList<CommandConfigElement> cmdList = new ArrayList<CommandConfigElement>();
//                Iterator<String> it = cmdIds.iterator();
//                while(it.hasNext()) {
//                    String cmdId = (String)it.next();
//                    CommandConfigElement cmde = (CommandConfigElement)pma.getEntity(cmdId, CommandConfigElement.class);
//                    if (cmde != null) {
//                        cmdList.add(cmde);
//                    } else {
//                        ClusterConfigResponse acr = new ClusterConfigResponse(new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
//                                "Command Does Not Exist: {" + cmdId +"}"));
//                        return ResponseUtil.createResponse(acr);
//                    }
//                }
//                ce.setCommands(cmdList);
//            }
//        }
        ClusterConfigResponse ccr = ccs.createClusterConfig(request);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Update/insert cluster configuration.
     *
     * @param id unique if for cluster to upsert
     * @param request contains the cluster config element for this cluster
     * @return successful response, or one with an HTTP error code
     */
    @PUT
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response updateClusterConfig(@PathParam("id") String id,
            ClusterConfigRequest request) {
        LOG.info("called to create/update cluster");

        Cluster clusterConfig = request.getClusterConfig();
        if (clusterConfig != null) {
            // include "id" in the request
            clusterConfig.setId(id);
//            ArrayList<String> cmdIds = clusterConfig.getCmdIds();
//
//            if(cmdIds != null) {
//                PersistenceManager<CommandConfigElement> pma = new PersistenceManager<CommandConfigElement>();
//                ArrayList<CommandConfigElement> cmdList = new ArrayList<CommandConfigElement>();
//                Iterator<String> it = cmdIds.iterator();
//                while(it.hasNext()) {
//                    String cmdId = (String)it.next();
//                    CommandConfigElement cmde = (CommandConfigElement)pma.getEntity(cmdId, CommandConfigElement.class);
//                    if (cmde != null) {
//                        cmdList.add(cmde);
//                    } else {
//                        ClusterConfigResponse acr = new ClusterConfigResponse(new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
//                                "Command Does Not Exist: {" + cmdId +"}"));
//                        return ResponseUtil.createResponse(acr);
//                    }
//                }
//                clusterConfig.setCommands(cmdList);
//            }
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
        LOG.info("called");
        // this will just return an error
        return deleteClusterConfig(null);
    }

    /**
     * Delete entry for cluster.
     *
     * @param id unique id for cluster to delete
     * @return successful response, or one with an HTTP error code
     */
    @DELETE
    @Path("/{id}")
    public Response deleteClusterConfig(@PathParam("id") String id) {
        LOG.info("delete called for id: " + id);
        ClusterConfigResponse ccr = ccs.deleteClusterConfig(id);
        return ResponseUtil.createResponse(ccr);
    }
    /**
     * Create Cluster configuration.
     *
     * @param request contains a cluster config element
     * @return successful response, or one with an HTTP error code
     */
//    @POST
//    @Path("/")
//    @Consumes({ "application/xml", "application/json" })
//    public Response createClusterConfig(ClusterConfigRequest request) {
//        logger.info("called to create new cluster");
//        //ClusterConfigResponseOld ccr = ccs.createClusterConfig(request);
//
//        logger.debug("Received request:" + request.getClusterConfig().getId());
//        ClusterConfigResponse cr = new ClusterConfigResponse();
//
//        PersistenceManager<Cluster> pm = new PersistenceManager<Cluster>();
//        PersistenceManager<CommandConfigElement> pmc = new PersistenceManager<CommandConfigElement>();
//
//        Cluster cle = request.getClusterConfig();
//
//        ArrayList<CommandConfigElement> cmdList = new ArrayList<CommandConfigElement>();
//        Iterator<String> it = cle.getCmdIds().iterator();
//        while(it.hasNext()) {
//            CommandConfigElement ce = (CommandConfigElement)pmc.getEntity((String)it.next(), CommandConfigElement.class);
//            cmdList.add(ce);
//        }
//
//        cle.setCommands(cmdList);
//        pm.createEntity(cle);
//        return ResponseUtil.createResponse(cr);
//    }

//    @GET
//    @Path("/")
//    public Response getClusterConfig () {
//        String table = Cluster.class.getName();
//
//        ClusterConfigResponse response = new ClusterConfigResponse();
//      /*  PersistenceManager<ApplicationConfigElement> pm = new PersistenceManager<ApplicationConfigElement>();
//        QueryBuilder builder = new QueryBuilder().table(table);
//        Object[] results = pm.query(builder);
//
//        if (results.length != 0) {
//            ApplicationConfigElement[] apps = new ApplicationConfigElement[results.length];
//            for (int i = 0; i < results.length; i++) {
//                apps[i] = (ApplicationConfigElement) results[i];
//                logger.debug("Results Array" + apps[i].getId());
//                logger.debug("Jars is"+ apps[i].getJars());
//            }
//
//           response.setApplications(apps);
//        }
//         */
//
//        //java.util.Map<Object,Object> map = new java.util.HashMap<Object,Object>();
//        EntityManagerFactory factory = Persistence.createEntityManagerFactory("genie");
//        EntityManager em = factory.createEntityManager();
//
//        Query q = em.createQuery("select  x from Cluster x");
//        List<Cluster> results = (List<Cluster>) q.getResultList();
//
//        Iterator<Cluster> it = results.iterator();
//        Cluster[] apps = new Cluster[10];
//        int i =0;
//        while(it.hasNext()) {
//            Cluster c = (Cluster)it.next();
//            apps[i] = c;
//            logger.debug(c.getId());
//            //logger.debug(c.toString());
//            logger.debug(c.toString());
//            i++;
//        }
//        response.setClusterConfigs(apps);
//        return ResponseUtil.createResponse(response);
//    }
}
