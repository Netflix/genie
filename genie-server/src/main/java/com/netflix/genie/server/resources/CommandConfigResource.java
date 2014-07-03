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
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.services.CommandConfigService;
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
 * Code for CommandConfigResource.
 *
 * @author amsharma
 * @author tgianos
 */
@Path("/v2/config/commands")
@Api(value = "/v2/config/commands", description = "Manage the available commands")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
@Named
public class CommandConfigResource {

    private static final Logger LOG = LoggerFactory
            .getLogger(CommandConfigResource.class);

    /**
     * The command service.
     */
    @Inject
    private CommandConfigService ccs;

    /**
     * Uri info for gathering information on the request.
     */
    @Context
    private UriInfo uriInfo;

    /**
     * Create a Command configuration.
     *
     * @param command The command configuration to create
     * @return The command created
     * @throws CloudServiceException
     */
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Create a command",
            notes = "Create a command from the supplied information.",
            response = Command.class)
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Command.class),
        @ApiResponse(code = 400, message = "Invalid required parameter supplied"),
        @ApiResponse(code = 409, message = "A command with the supplied id already exists")
    })
    public Response createCommand(
            @ApiParam(value = "The command to create.", required = true)
            final Command command) throws CloudServiceException {
        LOG.debug("called to create new command configuration " + command.toString());
        final Command createdCommand = this.ccs.createCommand(command);
        return Response.created(
                this.uriInfo.getAbsolutePathBuilder().path(createdCommand.getId()).build()).
                entity(createdCommand).
                build();
    }

    /**
     * Get Command configuration for given id.
     *
     * @param id unique id for command configuration
     * @return The command configuration
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find a command by id",
            notes = "Get the command by id if it exists",
            response = Command.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Command.class),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Command getCommand(
            @ApiParam(value = "Id of the command to get.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called");
        return this.ccs.getCommand(id);
    }

    /**
     * Get Command configuration based on user parameters.
     *
     * @param name name for command (optional)
     * @param userName the user who created the configuration (optional)
     * @param page The page to start one (optional)
     * @param limit the max number of results to return per page (optional)
     * @return All the Commands matching the criteria or all if no criteria
     */
    @GET
    @ApiOperation(
            value = "Find commands",
            notes = "Find commands by the submitted criteria.",
            response = Command.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Command.class)
    })
    public List<Command> getCommands(
            @ApiParam(value = "Name of the command.", required = false)
            @QueryParam("name")
            final String name,
            @ApiParam(value = "User who created the command.", required = false)
            @QueryParam("userName")
            final String userName,
            @ApiParam(value = "The page to start on.", required = false)
            @QueryParam("page")
            @DefaultValue("0") int page,
            @ApiParam(value = "Max number of results per page.", required = false)
            @QueryParam("limit")
            @DefaultValue("1024") int limit) {
        LOG.debug("Called");
        return this.ccs.getCommands(name, userName, page, limit);
    }

    /**
     * Update command configuration.
     *
     * @param id unique id for the configuration to update.
     * @param updateCommand the information to update the command with
     * @return The updated command
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Update a command",
            notes = "Update a command from the supplied information.",
            response = Command.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Command.class),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Command to update not found")
    })
    public Command updateCommand(
            @ApiParam(value = "Id of the command to update.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The command information to update.", required = true)
            final Command updateCommand) throws CloudServiceException {
        LOG.debug("Called to create/update comamnd config");
        return this.ccs.updateCommand(id, updateCommand);
    }

    /**
     * Delete all applications from database.
     *
     * @return All The deleted comamnd
     * @throws CloudServiceException
     */
    @DELETE
    @ApiOperation(
            value = "Delete all commands",
            notes = "Delete all available commands and get them back.",
            response = Command.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public List<Command> deleteAllCommands() throws CloudServiceException {
        LOG.debug("called");
        return this.ccs.deleteAllCommands();
    }

    /**
     * Delete a command.
     *
     * @param id unique id for configuration to delete
     * @return The deleted configuration
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}")
    @ApiOperation(
            value = "Delete an comamnd",
            notes = "Delete an command with the supplied id.",
            response = Command.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Command.class),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Command deleteCommand(
            @ApiParam(value = "Id of the command to delete.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called");
        return this.ccs.deleteCommand(id);
    }

    /**
     * Add new configuration files to a given command.
     *
     * @param id The id of the command to add the configuration file to. Not
     * null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @return The active configurations for this command.
     * @throws CloudServiceException
     */
    @POST
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new configuration files to a command",
            notes = "Add the supplied configuration files to the command with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Set<String> addConfigsForCommand(
            @ApiParam(value = "Id of the command to add configuration to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration files to add.", required = true)
            final Set<String> configs) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and config " + configs);
        return this.ccs.addConfigsForCommand(id, configs);
    }

    /**
     * Get all the configuration files for a given command.
     *
     * @param id The id of the command to get the configuration files for. Not
     * NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}/configs")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get the configuration files for a command",
            notes = "Get the configuration files for the command with the supplied id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Set<String> getConfigsForCommand(
            @ApiParam(value = "Id of the command to get configurations for.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.getConfigsForCommand(id);
    }

    /**
     * Update the configuration files for a given command.
     *
     * @param id The id of the command to update the configuration files for.
     * Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     * files with. Not null/empty/blank.
     * @return The new set of command configurations.
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update configuration files for an command",
            notes = "Replace the existing configuration files for command with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Set<String> updateConfigsForCommand(
            @ApiParam(value = "Id of the command to update configurations for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The configuration files to replace existing with.", required = true)
            final Set<String> configs) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and configs " + configs);
        return this.ccs.updateConfigsForCommand(id, configs);
    }

    /**
     * Delete the all configuration files from a given command.
     *
     * @param id The id of the command to delete the configuration files from.
     * Not null/empty/blank.
     * @return Empty set if successful
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/configs")
    @ApiOperation(
            value = "Remove all configuration files from an command",
            notes = "Remove all the configuration files from the command with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Set<String> removeAllConfigsForCommand(
            @ApiParam(value = "Id of the command to delete from.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.removeAllConfigsForCommand(id);
    }

    /**
     * Add new applications to the given command.
     *
     * @param id The id of the command to add the applications to. Not
     * null/empty/blank.
     * @param applications The applications to add. Not null.
     * @return The active applications for this command.
     * @throws CloudServiceException
     */
    @POST
    @Path("/{id}/applications")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Add new applications to a command",
            notes = "Add the supplied applications to the command with the supplied id."
            + " Applications should already have been created.",
            response = Application.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public List<Application> addApplicationsForCommand(
            @ApiParam(value = "Id of the command to add applications to.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The applications to add.", required = true)
            final List<Application> applications) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and applications " + applications);
        return this.ccs.addApplicationsForCommand(id, applications);
    }

    /**
     * Get all the applications configured for a given command.
     *
     * @param id The id of the command to get the application files for. Not
     * NULL/empty/blank.
     * @return The active set of applications for the command.
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}/applications")
    @ApiOperation(
            value = "Get the applications for a command",
            notes = "Get the applications for the command with the supplied id.",
            response = Application.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public List<Application> getApplicationsForCommand(
            @ApiParam(value = "Id of the command to get configurations for.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.getApplicationsForCommand(id);
    }

    /**
     * Update the applications for a given command.
     *
     * @param id The id of the command to update the configuration files for.
     * Not null/empty/blank.
     * @param applications The applications to replace existing applications
     * with. Not null/empty/blank.
     * @return The new set of applications for the command.
     * @throws CloudServiceException
     */
    @PUT
    @Path("/{id}/applications")
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(
            value = "Update the applications for an command",
            notes = "Replace the existing application files for command with given id.",
            response = Application.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public List<Application> updateApplicationsForCommand(
            @ApiParam(value = "Id of the command to update configurations for.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The applications to replace existing with. Should already be created",
                    required = true)
            final List<Application> applications) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and configs " + applications);
        return this.ccs.updateApplicationsForCommand(id, applications);
    }

    /**
     * Remove the all applications from a given command.
     *
     * @param id The id of the command to delete the applications from. Not
     * null/empty/blank.
     * @return Empty set if successful
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/applications")
    @ApiOperation(
            value = "Remove all applications from an command",
            notes = "Remove all the applications from the command with given id.",
            response = Application.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid Id supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public List<Application> removeAllApplicationsForCommand(
            @ApiParam(value = "Id of the command to delete from.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.removeAllApplicationsForCommand(id);
    }

    /**
     * Remove an application from a given command.
     *
     * @param id The id of the command to delete the application from. Not
     * null/empty/blank.
     * @param appId The id of the application to remove. Not null/empty/blank.
     * @return The active set of applications for the command.
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}/applications/{appId}")
    @ApiOperation(
            value = "Remove a configuration file from an command",
            notes = "Remove the given configuration file from the command with given id.",
            response = String.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public List<Application> removeApplicationForCommand(
            @ApiParam(value = "Id of the command to delete from.", required = true)
            @PathParam("id")
            final String id,
            @ApiParam(value = "The id of the application to remove.", required = true)
            @PathParam("appId")
            final String appId) throws CloudServiceException {
        LOG.debug("Called with id " + id + " and application id " + appId);
        return this.ccs.removeApplicationForCommand(id, appId);
    }

    /**
     * Get all the clusters this command is associated with.
     *
     * @param id The id of the command to get the clusters for. Not
     * NULL/empty/blank.
     * @return The set of clusters.
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}/clusters")
    @ApiOperation(
            value = "Get the clusters this command is associated with",
            notes = "Get the clusters which this command exists on supports.",
            response = Cluster.class,
            responseContainer = "Set")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK"),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Command not found")
    })
    public Set<Cluster> getClustersForCommand(
            @ApiParam(value = "Id of the command to get the clusters for.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("Called with id " + id);
        return this.ccs.getClustersForCommand(id);
    }
}
