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
import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;
import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ApplicationConfigElement;
import com.netflix.genie.common.model.ClusterConfigElement;
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
@Path("/v1/config/cluster")
@Produces({ "application/xml", "application/json" })
public class ClusterConfigResourceV1 {
    
    private static Logger logger = LoggerFactory
            .getLogger(ClusterConfigResourceV1.class);

    /**
     * Custom JAXB context resolver for the cluster config requests/responses.
     *
     * @author amsharma
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
    public ClusterConfigResourceV1() throws CloudServiceException {
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
    public Response createClusterConfig(ClusterConfigRequest request) {
        logger.info("called to create new cluster");
        //ClusterConfigResponseOld ccr = ccs.createClusterConfig(request);
        
        logger.debug("Received request:" + request.getCluster().getId());
        ClusterConfigResponse cr = new ClusterConfigResponse();
        
        PersistenceManager<ClusterConfigElement> pm = new PersistenceManager<ClusterConfigElement>();
        PersistenceManager<CommandConfigElement> pmc = new PersistenceManager<CommandConfigElement>();
        
        ClusterConfigElement cle = request.getCluster();
        
        ArrayList<CommandConfigElement> cmdList = new ArrayList<CommandConfigElement>();
        Iterator<String> it = cle.getCmds().iterator();
        while(it.hasNext()) {
            
            CommandConfigElement ce = (CommandConfigElement)pmc.getEntity((String)it.next(), CommandConfigElement.class);
            cmdList.add(ce);
        }
        
        cle.setCommands(cmdList); 
        pm.createEntity(cle);
        return ResponseUtil.createResponse(cr);
    }
    
    @GET
    @Path("/")
    public Response getClusterConfig () {
        String table = ClusterConfigElement.class.getName();
        
        ClusterConfigResponse response = new ClusterConfigResponse();
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
        
        Query q = em.createQuery("select  x from ClusterConfigElement x");
        List<ClusterConfigElement> results = (List<ClusterConfigElement>) q.getResultList();
        
        Iterator<ClusterConfigElement> it = results.iterator();
        ClusterConfigElement[] apps = new ClusterConfigElement[10];
        int i =0;
        while(it.hasNext()) {
            ClusterConfigElement c = (ClusterConfigElement)it.next();
            apps[i] = c;
            logger.debug(c.getId());
            //logger.debug(c.toString());
            logger.debug(c.toString());
            i++;
        } 
        response.setClusters(apps);
        return ResponseUtil.createResponse(response);
    }  
}
