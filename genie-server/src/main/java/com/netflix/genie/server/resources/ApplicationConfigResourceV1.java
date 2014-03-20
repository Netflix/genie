package com.netflix.genie.server.resources;

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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;
import com.netflix.genie.common.messages.ClusterConfigRequestOld;
import com.netflix.genie.common.messages.ClusterConfigResponseOld;
import com.netflix.genie.common.messages.HiveConfigResponse;
import com.netflix.genie.common.model.ApplicationConfigElement;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ApplicationConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Code for ApplicationConfigResource - REST end-point for supporting Application
 * @author amsharma
 * 
 */
@Path("/v1/config/application")
@Produces({ "application/xml", "application/json" })
public class ApplicationConfigResourceV1 {
    
    private ApplicationConfigService acs;
    private static Logger logger = LoggerFactory
            .getLogger(ApplicationConfigResourceV1.class);

    
    /**
     * Custom JAXB context resolver for the cluster config requests/responses.
     *
     * @author amsharma
     */
    @Provider
    public static class ApplicationJAXBContextResolver extends JAXBContextResolver {
        /**
         * Constructor - initialize the resolver for the types that
         * this resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public ApplicationJAXBContextResolver() throws Exception {
            super(new Class[]{ApplicationConfigElement.class,
                    ApplicationConfigRequest.class,
                    ApplicationConfigResponse.class});
        }
    }
    
    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     *             if there is any error
     */
    public ApplicationConfigResourceV1() throws CloudServiceException {
        acs = ConfigServiceFactory.getApplicationConfigImpl();
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
    public Response createApplicationConfig(ApplicationConfigRequest request) {
        logger.info("called to create new application");
        //ClusterConfigResponseOld ccr = ccs.createClusterConfig(request);
        
        logger.debug("Received request:" + request.getApplication().getId());
        ApplicationConfigResponse ar = new ApplicationConfigResponse();
        
        PersistenceManager<ApplicationConfigElement> pm = new PersistenceManager<ApplicationConfigElement>();
        ApplicationConfigElement ae = request.getApplication();
        pm.createEntity(ae);
        
        return ResponseUtil.createResponse(ar);
    }
    
    /**
     * Get Application config based on user params.
     *
     * @param id
     *            unique id for config (optional)
     * @param name
     *            name for config (optional)

     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/")
    public Response getApplicationConfig (@QueryParam("id") String id,
            @QueryParam("name") String name) {
        
        logger.info("called");
        ApplicationConfigResponse acr = acs.getApplicationConfig(id, name);
        return ResponseUtil.createResponse(acr);
        
        /* String table = ApplicationConfigElement.class.getName();
        
        ApplicationConfigResponse response = new ApplicationConfigResponse();
      PersistenceManager<ApplicationConfigElement> pm = new PersistenceManager<ApplicationConfigElement>();
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
         
        
        //java.util.Map<Object,Object> map = new java.util.HashMap<Object,Object>();
        EntityManagerFactory factory = Persistence.createEntityManagerFactory("genie");
        EntityManager em = factory.createEntityManager();
        
        Query q = em.createQuery("select  x from ApplicationConfigElement x");
        List<ApplicationConfigElement> results = (List<ApplicationConfigElement>) q.getResultList();
        
        Iterator<ApplicationConfigElement> it = results.iterator();
        ApplicationConfigElement[] apps = new ApplicationConfigElement[10];
        int i =0;
        while(it.hasNext()) {
            ApplicationConfigElement c = (ApplicationConfigElement)it.next();
            apps[i] = c;
            logger.debug(c.getId());
            logger.debug(c.getJars().toString());
            logger.debug(c.toString());
            i++;
        } 
        response.setApplications(apps);
        return ResponseUtil.createResponse(response); */
    }  
}
