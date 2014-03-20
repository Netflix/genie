package com.netflix.genie.server.resources;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;
import com.netflix.genie.common.messages.ClusterConfigRequestOld;
import com.netflix.genie.common.messages.ClusterConfigResponseOld;
import com.netflix.genie.common.model.ApplicationConfigElement;
import com.netflix.genie.common.model.CommandConfigElement;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Code for ApplicationConfigResource - REST end-point for supporting Application
 * @author amsharma
 */
@Path("/v1/config/command")
@Produces({ "application/xml", "application/json" })
public class CommandConfigResourceV1 {
    
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
        //ccs = ConfigServiceFactory.getClusterConfigImpl();
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
        //ClusterConfigResponseOld ccr = ccs.createClusterConfig(request);
        
        logger.debug("Received request:" + request.getCommand().getId());
        CommandConfigResponse ar = new CommandConfigResponse();
        
        PersistenceManager<CommandConfigElement> pm = new PersistenceManager<CommandConfigElement>();
        PersistenceManager<ApplicationConfigElement> pma = new PersistenceManager<ApplicationConfigElement>();
        
        CommandConfigElement ce = request.getCommand();
        //ae.setApplications();
        
        ArrayList<ApplicationConfigElement> appList = new ArrayList<ApplicationConfigElement>();
        Iterator<String> it = ce.getAppids().iterator();
        while(it.hasNext()) {
            ApplicationConfigElement ae = (ApplicationConfigElement)pma.getEntity((String)it.next(), ApplicationConfigElement.class);
            appList.add(ae);
        }
        ce.setApplications(appList);
        
        pm.createEntity(ce);
        return ResponseUtil.createResponse(ar);
    }
    
    @GET
    @Path("/")
    public Response getApplicationConfig () {
        String table = CommandConfigElement.class.getName();
        
        CommandConfigResponse response = new CommandConfigResponse();
      /*  PersistenceManager<ApplicationConfigElement> pm = new PersistenceManager<ApplicationConfigElement>();
        QueryBuilder builder = new QueryBuilder().table(table);
        Object[] results = pm.query(builder);
        
        if (results.length != 0) {
            ApplicationConfigElement[] apps = new ApplicationConfigElement[results.length];
            for (int i = 0; i < results.length; i++) {
                apps[i] = (ApplicationConfigElement) results[i];
                logger.debug("Results Array" + apps[i].getId());
                logger.debug("Jars is"+ apps[i].getJars());  
            }
            
           response.setApplications(apps);
        }
         */
        
        //java.util.Map<Object,Object> map = new java.util.HashMap<Object,Object>();
        EntityManagerFactory factory = Persistence.createEntityManagerFactory("genie");
        EntityManager em = factory.createEntityManager();
        
        Query q = em.createQuery("select  x from CommandConfigElement x");
        List<CommandConfigElement> results = (List<CommandConfigElement>) q.getResultList();
        
        Iterator<CommandConfigElement> it = results.iterator();
        CommandConfigElement[] apps = new CommandConfigElement[10];
        int i =0;
        while(it.hasNext()) {
            CommandConfigElement c = (CommandConfigElement)it.next();
            apps[i] = c;
            logger.debug(c.getId());
            //logger.debug(c.getJars().toString());
            logger.debug(c.toString());
            i++;
        } 
        response.setCommands(apps);
        return ResponseUtil.createResponse(response);
    }  
}
