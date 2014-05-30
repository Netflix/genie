package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
 * Code for CommandConfigResource - REST end-point for supporting Command.
 *
 * @author amsharma
 *
 */
@Path("/v1/config/commands")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
public class CommandConfigResourceV1 {

    private final CommandConfigService ccs;
    private static final Logger LOG = LoggerFactory
            .getLogger(CommandConfigResourceV1.class);

    /**
     * Custom JAXB context resolver for the cluster config requests/responses.
     *
     * @author amsharma
     */
    @Provider
    public static class CommandJAXBContextResolver extends JAXBContextResolver {

        /**
         * Constructor - initialize the resolver for the types that this
         * resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public CommandJAXBContextResolver() throws Exception {
            super(new Class[]{Command.class,
                CommandConfigRequest.class,
                CommandConfigResponse.class});
        }
    }

    /**
     * Default constructor.
     *
     * @throws CloudServiceException if there is any error
     */
    public CommandConfigResourceV1() throws CloudServiceException {
        ccs = ConfigServiceFactory.getCommandConfigImpl();
    }

    /**
     * Get Command config for given id.
     *
     * @param id unique id for command config
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/{id}")
    public Response getCommandConfig(@PathParam("id") String id) {
        LOG.info("called");
        return getCommandConfig(id, null);
    }

    /**
     * Get Command config based on user params.
     *
     * @param id unique id for config (optional)
     * @param name name for config (optional)
     *
     * @return successful response, or one with an HTTP error code
     */
    @GET
    @Path("/")
    public Response getCommandConfig(@QueryParam("id") String id,
            @QueryParam("name") String name) {

        LOG.info("called");
        CommandConfigResponse ccr = ccs.getCommandConfig(id, name);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Create Command configuration.
     *
     * @param request contains a command config element
     * @return successful response, or one with an HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response createCommandConfig(CommandConfigRequest request) {
        LOG.info("called to create new cluster");

        CommandConfigResponse ccr = ccs.createCommandConfig(request);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Insert/update command config.
     *
     * @param id unique id for config to upsert
     * @param request contains the command config element for update
     * @return successful response, or one with an HTTP error code
     */
    @PUT
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Response updateCommandConfig(@PathParam("id") String id,
            CommandConfigRequest request) {
        LOG.info("called to create/update comamnd config");
        Command commandConfig = request.getCommandConfig();
        if (commandConfig != null) {
            // include "id" in the request
            commandConfig.setId(id);
            ArrayList<String> appids = commandConfig.getAppIds();

            if (appids != null) {
                PersistenceManager<Application> pma = new PersistenceManager<Application>();
                ArrayList<Application> appList = new ArrayList<Application>();
                Iterator<String> it = appids.iterator();
                while (it.hasNext()) {
                    String appId = (String) it.next();
                    Application ae = (Application) pma.getEntity(appId, Application.class);
                    if (ae != null) {
                        appList.add(ae);
                    } else {
                        CommandConfigResponse acr = new CommandConfigResponse(new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                                "Application Does Not Exist: {" + appId + "}"));
                        return ResponseUtil.createResponse(acr);
                    }
                }
                commandConfig.setApplications(appList);
            }
        }

        CommandConfigResponse ccr = ccs.updateCommandConfig(request);
        return ResponseUtil.createResponse(ccr);
    }

    /**
     * Delete without an id, returns an error.
     *
     * @return error code, since no id is provided
     */
    @DELETE
    @Path("/")
    public Response deleteCommandConfig() {
        LOG.info("called");
        return deleteCommandConfig(null);
    }

    /**
     * Delete a command config from database.
     *
     * @param id unique id for config to delete
     * @return successful response, or one with an HTTP error code
     */
    @DELETE
    @Path("/{id}")
    public Response deleteCommandConfig(@PathParam("id") String id) {
        LOG.info("called");
        CommandConfigResponse ccr = ccs.deleteCommandConfig(id);
        return ResponseUtil.createResponse(ccr);
    }
}
