package com.netflix.genie.server.resources;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
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
import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;
import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;
import com.netflix.genie.common.messages.ClusterConfigRequestOld;
import com.netflix.genie.common.messages.ClusterConfigResponseOld;
import com.netflix.genie.common.model.ApplicationConfigElement;
import com.netflix.genie.common.model.CommandConfigElement;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ApplicationConfigService;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Code for ApplicationConfigResource - REST end-point for supporting Application
 * @author amsharma
 */
@Path("/v1/config/command")
@Produces({ "application/xml", "application/json" })
public class CommandConfigResourceV1 {
    
    private CommandConfigService ccs;
    private static Logger logger = LoggerFactory
            .getLogger(CommandConfigResourceV1.class);

    /**
     * Custom JAXB context resolver for the cluster config requests/responses.
     *
     * @author amsharma
     */
    @Provider
    public static class CommandJAXBContextResolver extends JAXBContextResolver {
        /**
         * Constructor - initialize the resolver for the types that
         * this resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public CommandJAXBContextResolver() throws Exception {
            super(new Class[]{CommandConfigElement.class,
                    CommandConfigRequest.class,
                    CommandConfigResponse.class});
        }
    }
    
    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     *             if there is any error
     */
    public CommandConfigResourceV1() throws CloudServiceException {
        ccs = ConfigServiceFactory.getCommandConfigImpl();
    }
    
    /**
     * Get Command config for given id.
     *
     * @param id
     *            unique id for application config
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/{id}")
    public Response getCommandConfig(@PathParam("id") String id) {
        logger.info("called");
        return getCommandConfig(id, null);
    }
    
    /**
     * Get Command config based on user params.
     *
     * @param id
     *            unique id for config (optional)
     * @param name
     *            name for config (optional)

     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/")
    public Response getCommandConfig (@QueryParam("id") String id,
            @QueryParam("name") String name) {
        
        logger.info("called");
        CommandConfigResponse ccr = ccs.getCommandConfig(id, name);
        return ResponseUtil.createResponse(ccr);
//        
//     EntityManagerFactory factory = Persistence.createEntityManagerFactory("genie");
//     EntityManager em = factory.createEntityManager();
//      
//      Query q = em.createQuery("select  x from CommandConfigElement x");
//      List<CommandConfigElement> results = (List<CommandConfigElement>) q.getResultList();
//      
//      Iterator<CommandConfigElement> it = results.iterator();
//      CommandConfigElement[] apps = new CommandConfigElement[10];
//      int i =0;
//      while(it.hasNext()) {
//          CommandConfigElement c = (CommandConfigElement)it.next();
//          c.getApplications();
//          apps[i] = c;
//          logger.debug(c.getId());
//          //logger.debug(c.getJars().toString());
//          logger.debug(c.toString());
//          i++;
//      } 
//      
//      CommandConfigResponse response = new CommandConfigResponse();
//      response.setCommandConfigs(apps);
//      return ResponseUtil.createResponse(response);
    }
    
    /**
     * Create Application configuration.
     *
     * @param request
     *            contains a application config element 
     * @return successful response, or one with an HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({ "application/xml", "application/json" })
    public Response createCommandConfig(CommandConfigRequest request) {
        logger.info("called to create new cluster");
        
        // Need to get the CommandConfig object and fetch the applcation objects from the DB 
        // to set it in the object. 
        
        CommandConfigElement ce = request.getCommandConfig();
        fetchAndAddApplications(ce);
        
        request.setCommandConfig(ce);
        CommandConfigResponse acr = ccs.createCommandConfig(request);
        return ResponseUtil.createResponse(acr);
        
        /*
        return 
        ClusterConfigResponseOld ccr = ccs.createClusterConfig(request);
        
        logger.debug("Received request:" + request.getCommandConfig().getId());
        CommandConfigResponse ar = new CommandConfigResponse();
        
        PersistenceManager<CommandConfigElement> pm = new PersistenceManager<CommandConfigElement>();
        
        
        CommandConfigElement ce = request.getCommandConfig();
        //ce.setApplicationsFromAppids();
        
        ArrayList<ApplicationConfigElement> appList = new ArrayList<ApplicationConfigElement>();
        Iterator<String> it = ce.getAppids().iterator();
        while(it.hasNext()) {
            ApplicationConfigElement ae = (ApplicationConfigElement)pma.getEntity((String)it.next(), ApplicationConfigElement.class);
            appList.add(ae);
        }
        ce.setApplications(appList);
        
        pm.createEntity(ce);
        return ResponseUtil.createResponse(ar);*/
    }
    
//    @GET
//    @Path("/")
//    public Response getApplicationConfig () {
//        String table = CommandConfigElement.class.getName();
//        
//        CommandConfigResponse response = new CommandConfigResponse();
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
//        Query q = em.createQuery("select  x from CommandConfigElement x");
//        List<CommandConfigElement> results = (List<CommandConfigElement>) q.getResultList();
//        
//        Iterator<CommandConfigElement> it = results.iterator();
//        CommandConfigElement[] apps = new CommandConfigElement[10];
//        int i =0;
//        while(it.hasNext()) {
//            CommandConfigElement c = (CommandConfigElement)it.next();
//            apps[i] = c;
//            logger.debug(c.getId());
//            //logger.debug(c.getJars().toString());
//            logger.debug(c.toString());
//            i++;
//        } 
//        response.setCommands(apps);
//        return ResponseUtil.createResponse(response);
//    }  
    
    /**
     * Insert/update command config.
     *
     * @param id
     *            unique id for config to upsert
     * @param request
     *            contains the comamnd config element for update
     * @return successful response, or one with an HTTP error code
     */
    @PUT
    @Path("/{id}")
    @Consumes({ "application/xml", "application/json" })
    public Response updateCommandConfig(@PathParam("id") String id,
            CommandConfigRequest request) {
        logger.info("called to create/update comamnd config");
        CommandConfigElement commandConfig = request.getCommandConfig();
        if (commandConfig != null) {
            // include "id" in the request
            commandConfig.setId(id);
            fetchAndAddApplications(commandConfig);
        }

        CommandConfigResponse ccr = ccs.updateCommandConfig(request);
        return ResponseUtil.createResponse(ccr);
    }
    
    private void fetchAndAddApplications(CommandConfigElement commandConfig) {
        
        ArrayList<String> appids = commandConfig.getAppids();
        
        // TODO Error handling in case apps are not present
        if(appids != null) {
            PersistenceManager<ApplicationConfigElement> pma = new PersistenceManager<ApplicationConfigElement>();
            ArrayList<ApplicationConfigElement> appList = new ArrayList<ApplicationConfigElement>();
            Iterator<String> it = appids.iterator();
            while(it.hasNext()) {
                ApplicationConfigElement ae = (ApplicationConfigElement)pma.getEntity((String)it.next(), ApplicationConfigElement.class);
                appList.add(ae);
            }
            commandConfig.setApplications(appList);
        }
    }

    /**
     * Delete without an id, returns an error.
     *
     * @return error code, since no id is provided
     */
    @DELETE
    @Path("/")
    public Response deleteCommandConfig() {
        logger.info("called");
        return deleteCommandConfig(null);
    }
    
    /**
     * Delete a command config from database.
     *
     * @param id
     *            unique id for config to delete
     * @return successful response, or one with an HTTP error code
     */
    @DELETE
    @Path("/{id}")
    public Response deleteCommandConfig(@PathParam("id") String id) {
        logger.info("called");
        CommandConfigResponse ccr = ccs.deleteCommandConfig(id);
        return ResponseUtil.createResponse(ccr);
    }
}
