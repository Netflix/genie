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
import com.netflix.genie.common.model.Application;
import com.netflix.genie.server.services.ApplicationConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for ApplicationConfigResource - REST end-point for supporting
 * Application.
 *
 * @author amsharma
 * @author tgianos
 */
@Path("/v1/config/applications")
@Api(value = "/v1/config/applications", description = "Manage the available applicationss")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
public class ApplicationConfigResourceV1 {

    private final ApplicationConfigService acs;
    private static final Logger LOG = LoggerFactory
            .getLogger(ApplicationConfigResourceV1.class);

    /**
     * Uri info for gathering information on the request
     */
    @Context
    private UriInfo uriInfo;

    /**
     * Default constructor.
     *
     * @throws CloudServiceException if there is any error
     */
    public ApplicationConfigResourceV1() throws CloudServiceException {
        this.acs = ConfigServiceFactory.getApplicationConfigImpl();
    }

    /**
     * Get Application configuration for given id.
     *
     * @param id unique id for application configuration
     * @return The application configuration
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find an application by id",
            notes = "More notes about this method",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Application getApplicationConfig(
            @ApiParam(value = "ID of the application to get.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called");
        return this.acs.getApplicationConfig(id);
    }

    /**
     * Get Application configuration based on user parameters.
     *
     * @param name name for configuration (optional)
     * @param userName the user who created the application (optional)
     * @param page The page to start one (optional)
     * @param limit the max number of results to return per page (optional)
     * @return All applications matching the criteria
     */
    @GET
    @ApiOperation(
            value = "Find applications",
            notes = "Find applications by the submitted criteria.",
            response = Application.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Application.class)})
    public List<Application> getApplicationConfigs(
            @ApiParam(value = "Name of the application.", required = false)
            @QueryParam("name")
            final String name,
            @ApiParam(value = "User who created the application.", required = false)
            @QueryParam("userName")
            final String userName,
            @ApiParam(value = "The page to start on.", required = false)
            @QueryParam("page")
            @DefaultValue("0") int page,
            @ApiParam(value = "Max number of results per page.", required = false)
            @QueryParam("limit")
            @DefaultValue("1024") int limit) {
        LOG.debug("called");
        return this.acs.getApplicationConfigs(name, userName, page, limit);
    }

    /**
     * Create an Application configuration.
     *
     * @param app The application to create
     * @return The created application configuration
     * @throws CloudServiceException
     */
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ApiOperation(
            value = "Create an application",
            notes = "Create an application from the supplied information.",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid required parameter supplied"),
        @ApiResponse(code = 409, message = "An application with the supplied id already exists")
    })
    public Response createApplicationConfig(
            @ApiParam(value = "The application to create.", required = true)
            final Application app) throws CloudServiceException {
        LOG.debug("Called to create new application");
        final Application createdApp = this.acs.createApplicationConfig(app);
        return Response.created(
                this.uriInfo.getAbsolutePathBuilder().path(createdApp.getId()).build()).
                entity(createdApp).
                build();
    }

    /**
     * Update application configuration.
     *
     * @param id unique id for configuration to update
     * @param updateApp contains the application information to update
     * @return successful response, or one with an HTTP error code
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ApiOperation(
            value = "Update an application",
            notes = "Update an application from the supplied information.",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application to update not found")
    })
    public Application updateApplicationConfig(
            @ApiParam(value = "Id of the application to update.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The application information to update.", required = true)
            final Application updateApp) throws CloudServiceException {
        LOG.debug("called to update application config with info " + updateApp.toString());
        return this.acs.updateApplicationConfig(id, updateApp);
    }

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of configuration to delete
     * @return The deleted application configuration
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}")
    @ApiOperation(
            value = "Delete an application",
            notes = "Delete an application with the supplied id.",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Application deleteApplicationConfig(
            @ApiParam(value = "Id of the application to delete.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("called");
        return this.acs.deleteApplicationConfig(id);
    }
}
