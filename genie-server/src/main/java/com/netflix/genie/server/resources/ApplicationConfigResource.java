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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.services.ApplicationConfigService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
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
@Path("/v2/config/applications")
@Api(value = "/v2/config/applications", description = "Manage the available applications")
@Produces({
    MediaType.APPLICATION_XML,
    MediaType.APPLICATION_JSON
})
@Named
public class ApplicationConfigResource {

    private static final Logger LOG = LoggerFactory
            .getLogger(ApplicationConfigResource.class);

    /**
     * The application service.
     */
    private final ApplicationConfigService acs;

    /**
     * Uri info for gathering information on the request.
     */
    @Context
    private UriInfo uriInfo;

    /**
     * Constructor.
     *
     * @param acs The application configuration service to use.
     */
    @Inject
    public ApplicationConfigResource(final ApplicationConfigService acs) {
        this.acs = acs;
    }

    /**
     * Create an Application.
     *
     * @param app The application to create
     * @return The created application configuration
     * @throws GenieException
     */
    @POST
    @Consumes({
        MediaType.APPLICATION_XML,
        MediaType.APPLICATION_JSON
    })
    @ApiOperation(
            value = "Create an application",
            notes = "Create an application from the supplied information.",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid required parameter supplied"),
        @ApiResponse(code = 409, message = "An application with the supplied id already exists")
    })
    public Response createApplication(
            @ApiParam(value = "The application to create.", required = true)
            final Application app) throws GenieException {
        LOG.debug("Called to create new application");
        final Application createdApp = this.acs.createApplication(app);
        return Response.created(
                this.uriInfo.getAbsolutePathBuilder().path(createdApp.getId()).build()).
                entity(createdApp).
                build();
    }

    /**
     * Get Application for given id.
     *
     * @param id unique id for application configuration
     * @return The application configuration
     * @throws GenieException
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find an application by id",
            notes = "Get the application by id if it exists",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Application getApplication(
            @ApiParam(value = "Id of the application to get.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called");
        return this.acs.getApplication(id);
    }

    /**
     * Get Applications based on user parameters.
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
        @ApiResponse(code = 200, message = "OK", response = Application.class)
    })
    public List<Application> getApplications(
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
        return this.acs.getApplications(name, userName, page, limit);
    }

    /**
     * Update application.
     *
     * @param id unique id for configuration to update
     * @param updateApp contains the application information to update
     * @return successful response, or one with an HTTP error code
     * @throws GenieException
     */
    @PUT
    @Path("/{id}")
    @Consumes({
        MediaType.APPLICATION_XML,
        MediaType.APPLICATION_JSON
    })
    @ApiOperation(
            value = "Update an application",
            notes = "Update an application from the supplied information.",
            response = Application.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Application.class),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application to update not found")
    })
    public Application updateApplication(
            @ApiParam(value = "Id of the application to update.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The application information to update.", required = true)
            final Application updateApp) throws GenieException {
        LOG.debug("called to update application config with info " + updateApp.toString());
        return this.acs.updateApplication(id, updateApp);
    }

    /**
     * Delete all applications from database.
     *
     * @return All The deleted applications
     * @throws GenieException
     */
    @DELETE
    @ApiOperation(
            value = "Delete all applications",
            notes = "Delete all available applications and get them back.",
            response = Application.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public List<Application> deleteAllApplications() throws GenieException {
        LOG.debug("called");
        return this.acs.deleteAllApplications();
    }

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of configuration to delete
     * @return The deleted application configuration
     * @throws GenieException
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
    public Application deleteApplication(
            @ApiParam(value = "Id of the application to delete.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("called");
        return this.acs.deleteApplication(id);
    }

    /**
     * Add new configuration files to a given application.
     *
     * @param id The id of the application to add the configuration file to. Not
     * null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @return The active configurations for this application.
     * @throws GenieException
     */
    @POST
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new configuration files to an application",
            notes = "Add the supplied configuration files to the applicaiton with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> addConfigsToApplication(
            @ApiParam(value = "Id of the application to add configuration to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration files to add.", required = true)
            final Set<String> configs) throws GenieException {
        LOG.debug("Called with id " + id + " and config " + configs);
        return this.acs.addConfigsToApplication(id, configs);
    }

    /**
     * Get all the configuration files for a given application.
     *
     * @param id The id of the application to get the configuration files for.
     * Not NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException
     */
    @GET
    @Path("/{id}/configs")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get the configuration files for an application",
            notes = "Get the configuration files for the application with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> getConfigsForApplication(
            @ApiParam(value = "Id of the application to get configurations for.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.getConfigsForApplication(id);
    }

    /**
     * Update the configuration files for a given application.
     *
     * @param id The id of the application to update the configuration files
     * for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     * files with. Not null/empty/blank.
     * @return The new set of application configurations.
     * @throws GenieException
     */
    @PUT
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update configuration files for an application",
            notes = "Replace the existing configuration files for application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> updateConfigsForApplication(
            @ApiParam(value = "Id of the application to update configurations for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration files to replace existing with.", required = true)
            final Set<String> configs) throws GenieException {
        LOG.debug("Called with id " + id + " and configs " + configs);
        return this.acs.updateConfigsForApplication(id, configs);
    }

    /**
     * Delete the all configuration files from a given application.
     *
     * @param id The id of the application to delete the configuration files
     * from. Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    @DELETE
    @Path("/{id}/configs")
    @ApiOperation(
            value = "Remove all configuration files from an application",
            notes = "Remove all the configuration files from the application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> removeAllConfigsForApplication(
            @ApiParam(value = "Id of the application to delete from.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.removeAllConfigsForApplication(id);
    }

    /**
     * Add new jar files for a given application.
     *
     * @param id The id of the application to add the jar file to. Not
     * null/empty/blank.
     * @param jars The jar files to add. Not null.
     * @return The active set of application jars.
     * @throws GenieException
     */
    @POST
    @Path("/{id}/jars")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new jar files to an application",
            notes = "Add the supplied jar files to the applicaiton with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> addJarsForApplication(
            @ApiParam(value = "Id of the application to add jar to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The jar files to add.", required = true)
            final Set<String> jars) throws GenieException {
        LOG.debug("Called with id " + id + " and jars " + jars);
        return this.acs.addJarsForApplication(id, jars);
    }

    /**
     * Get all the jar files for a given application.
     *
     * @param id The id of the application to get the jar files for. Not
     * NULL/empty/blank.
     * @return The set of jar files.
     * @throws GenieException
     */
    @GET
    @Path("/{id}/jars")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get the jars for an application",
            notes = "Get the jars for the application with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> getJarsForApplication(
            @ApiParam(value = "Id of the application to get the jars for.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.getJarsForApplication(id);
    }

    /**
     * Update the jar files for a given application.
     *
     * @param id The id of the application to update the jar files for. Not
     * null/empty/blank.
     * @param jars The jar files to replace existing jar files with. Not
     * null/empty/blank.
     * @return The active set of application jars
     * @throws GenieException
     */
    @PUT
    @Path("/{id}/jars")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update jar files for an application",
            notes = "Replace the existing jar files for application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> updateJarsForApplication(
            @ApiParam(value = "Id of the application to update configurations for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The jar files to replace existing with.", required = true)
            final Set<String> jars) throws GenieException {
        LOG.debug("Called with id " + id + " and jars " + jars);
        return this.acs.updateJarsForApplication(id, jars);
    }

    /**
     * Delete the all jar files from a given application.
     *
     * @param id The id of the application to delete the jar files from. Not
     * null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    @DELETE
    @Path("/{id}/jars")
    @ApiOperation(
            value = "Remove all jar files from an application",
            notes = "Remove all the jar files from the application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> removeAllJarsForApplication(
            @ApiParam(value = "Id of the application to delete from.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.removeAllJarsForApplication(id);
    }
    
    /**
     * Add new tags to a given application.
     *
     * @param id The id of the application to add the tags to. Not
     * null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @return The active tags for this application.
     * @throws GenieException
     */
    @POST
    @Path("/{id}/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new tags to a application",
            notes = "Add the supplied tags to the application with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> addTagsForApplication(
            @ApiParam(value = "Id of the application to add configuration to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The tags to add.", required = true)
            final Set<String> tags) throws GenieException {
        LOG.debug("Called with id " + id + " and config " + tags);
        return this.acs.addTagsForApplication(id, tags);
    }

    /**
     * Get all the tags for a given application.
     *
     * @param id The id of the application to get the tags for. Not
     * NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException
     */
    @GET
    @Path("/{id}/tags")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get the tags for a application",
            notes = "Get the tags for the application with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> getTagsForApplication(
            @ApiParam(value = "Id of the application to get tags for.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.getTagsForApplication(id);
    }

    /**
     * Update the tags for a given application.
     *
     * @param id The id of the application to update the tags for.
     * Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     * files with. Not null/empty/blank.
     * @return The new set of application tags.
     * @throws GenieException
     */
    @PUT
    @Path("/{id}/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update tags for a application",
            notes = "Replace the existing tags for application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> updateTagsForApplication(
            @ApiParam(value = "Id of the application to update tags for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The tags to replace existing with.", required = true)
            final Set<String> tags) throws GenieException {
        LOG.debug("Called with id " + id + " and tags " + tags);
        return this.acs.updateTagsForApplication(id, tags);
    }

    /**
     * Delete the all tags from a given application.
     *
     * @param id The id of the application to delete the tags from.
     * Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    @DELETE
    @Path("/{id}/tags")
    @ApiOperation(
            value = "Remove all tags from a application",
            notes = "Remove all the tags from the application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> removeAllTagsForApplication(
            @ApiParam(value = "Id of the application to delete from.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.removeAllTagsForApplication(id);
    }

    /**
     * Get all the commands this application is associated with.
     *
     * @param id The id of the application to get the commands for. Not
     * NULL/empty/blank.
     * @return The set of commands.
     * @throws GenieException
     */
    @GET
    @Path("/{id}/commands")
    @ApiOperation(
            value = "Get the commands this application is associated with",
            notes = "Get the commands which this application supports.",
            response = Command.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<Command> getCommandsForApplication(
            @ApiParam(value = "Id of the application to get the commands for.", required = true)
            @PathParam("id")
            final String id) throws GenieException {
        LOG.debug("Called with id " + id);
        return this.acs.getCommandsForApplication(id);
    }

    /**
     * Remove an tag from a given application.
     *
     * @param id The id of the application to delete the tag from. Not
     * null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags for the application.
     * @throws GenieException
     */
    @DELETE
    @Path("/{id}/tags/{tag}")
    @ApiOperation(
            value = "Remove a tag from a application",
            notes = "Remove the given tag from the application with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Application not found")
    })
    public Set<String> removeTagForApplication(
            @ApiParam(value = "Id of the application to delete from.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The tag to remove.", required = true)
            @PathParam("tag")
            final String tag) throws GenieException {
        LOG.debug("Called with id " + id + " and tag " + tag);
        return this.acs.removeTagForApplication(id, tag);
    }
}
