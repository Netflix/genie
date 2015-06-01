/*
 *
 *  Copyright 2015 Netflix, Inc.
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
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.services.ApplicationConfigService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

import java.net.HttpURLConnection;
import java.util.EnumSet;
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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for ApplicationConfigResource - REST end-point for supporting
 * Application.
 *
 * @author amsharma
 * @author tgianos
 */
@Named
@Path("/v2/config/applications")
@Api(
        value = "/v2/config/applications",
        tags = "applications",
        description = "Manage the available applications"
)
@Produces(MediaType.APPLICATION_JSON)
public final class ApplicationConfigResource {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationConfigResource.class);

    /**
     * The application service.
     */
    private final ApplicationConfigService applicationConfigService;

    /**
     * To get URI information for return codes.
     */
    @Context
    private UriInfo uriInfo;

    /**
     * Constructor.
     *
     * @param applicationConfigService The application configuration service to use.
     */
    @Inject
    public ApplicationConfigResource(final ApplicationConfigService applicationConfigService) {
        this.applicationConfigService = applicationConfigService;
    }

    /**
     * Create an Application.
     *
     * @param app The application to create
     * @return The created application configuration
     * @throws GenieException For any error
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Create an application",
            notes = "Create an application from the supplied information.",
            response = Application.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CREATED,
                    message = "Application created successfully.",
                    response = Application.class
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CONFLICT,
                    message = "An application with the supplied id already exists"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "A precondition failed"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Response createApplication(
            @ApiParam(
                    value = "The application to create.",
                    required = true
            )
            final Application app
    ) throws GenieException {
        LOG.info("Called to create new application");
        final Application createdApp = this.applicationConfigService.createApplication(app);
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
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find an application by id",
            notes = "Get the application by id if it exists",
            response = Application.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_OK,
                    message = "OK",
                    response = Application.class
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid id supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Application getApplication(
            @ApiParam(
                    value = "Id of the application to get.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called to get Application for id " + id);
        return this.applicationConfigService.getApplication(id);
    }

    /**
     * Get Applications based on user parameters.
     *
     * @param name       name for configuration (optional)
     * @param userName   The user who created the application (optional)
     * @param statuses   The statuses of the applications (optional)
     * @param tags       The set of tags you want the command for.
     * @param page       The page to start one (optional)
     * @param limit      the max number of results to return per page (optional)
     * @param descending Whether results returned in descending or ascending order (optional)
     * @param orderBys   The fields to order the results by (optional)
     * @return All applications matching the criteria
     * @throws GenieException For any error
     */
    @GET
    @ApiOperation(
            value = "Find applications",
            notes = "Find applications by the submitted criteria.",
            response = Application.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "If status is invalid."
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Application> getApplications(
            @ApiParam(
                    value = "Name of the application."
            )
            @QueryParam("name")
            final String name,
            @ApiParam(
                    value = "User who created the application."
            )
            @QueryParam("userName")
            final String userName,
            @ApiParam(
                    value = "The status of the applications to get.",
                    allowableValues = "ACTIVE, DEPRECATED, INACTIVE"
            )
            @QueryParam("status")
            final Set<String> statuses,
            @ApiParam(
                    value = "Tags for the cluster."
            )
            @QueryParam("tag")
            final Set<String> tags,
            @ApiParam(
                    value = "The page to start on."
            )
            @QueryParam("page")
            @DefaultValue("0")
            int page,
            @ApiParam(
                    value = "Max number of results per page."
            )
            @QueryParam("limit")
            @DefaultValue("1024")
            int limit,
            @ApiParam(
                    value = "Whether results should be sorted in descending or ascending order. Defaults to descending"
            )
            @QueryParam("descending")
            @DefaultValue("true")
            boolean descending,
            @ApiParam(
                    value = "The fields to order the results by. Must not be collection fields. Default is updated."
            )
            @QueryParam("orderBy")
            final Set<String> orderBys
    ) throws GenieException {
        LOG.info(
                "Called [name | userName | status | tags | page | limit | descending | orderBys]"
        );
        LOG.info(
                name
                        + " | "
                        + userName
                        + " | "
                        + statuses
                        + " | "
                        + tags
                        + " | "
                        + page
                        + " | "
                        + limit
                        + " | "
                        + descending
                        + " | "
                        + orderBys
        );
        Set<ApplicationStatus> enumStatuses = null;
        if (!statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(ApplicationStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(ApplicationStatus.parse(status));
                }
            }
        }
        return this.applicationConfigService.getApplications(
                name, userName, enumStatuses, tags, page, limit, descending, orderBys);
    }

    /**
     * Update application.
     *
     * @param id        unique id for configuration to update
     * @param updateApp contains the application information to update
     * @return successful response, or one with an HTTP error code
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update an application",
            notes = "Update an application from the supplied information.",
            response = Application.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application to update not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Application updateApplication(
            @ApiParam(
                    value = "Id of the application to update.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The application information to update.",
                    required = true
            )
            final Application updateApp
    ) throws GenieException {
        LOG.info("called to update application config with info " + updateApp.toString());
        return this.applicationConfigService.updateApplication(id, updateApp);
    }

    /**
     * Delete all applications from database.
     *
     * @return All The deleted applications
     * @throws GenieException For any error
     */
    @DELETE
    @ApiOperation(
            value = "Delete all applications",
            notes = "Delete all available applications and get them back.",
            response = Application.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Application> deleteAllApplications() throws GenieException {
        LOG.info("Delete all Applications");
        return this.applicationConfigService.deleteAllApplications();
    }

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of configuration to delete
     * @return The deleted application configuration
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}")
    @ApiOperation(
            value = "Delete an application",
            notes = "Delete an application with the supplied id.",
            response = Application.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Application deleteApplication(
            @ApiParam(
                    value = "Id of the application to delete.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Delete an application with id " + id);
        return this.applicationConfigService.deleteApplication(id);
    }

    /**
     * Add new configuration files to a given application.
     *
     * @param id      The id of the application to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @return The active configurations for this application.
     * @throws GenieException For any error
     */
    @POST
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new configuration files to an application",
            notes = "Add the supplied configuration files to the application with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> addConfigsToApplication(
            @ApiParam(
                    value = "Id of the application to add configuration to.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to add.",
                    required = true
            )
            final Set<String> configs
    ) throws GenieException {
        LOG.info("Called with id " + id + " and config " + configs);
        return this.applicationConfigService.addConfigsToApplication(id, configs);
    }

    /**
     * Get all the configuration files for a given application.
     *
     * @param id The id of the application to get the configuration files for.
     *           Not NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/configs")
    @ApiOperation(
            value = "Get the configuration files for an application",
            notes = "Get the configuration files for the application with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getConfigsForApplication(
            @ApiParam(
                    value = "Id of the application to get configurations for.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.applicationConfigService.getConfigsForApplication(id);
    }

    /**
     * Update the configuration files for a given application.
     *
     * @param id      The id of the application to update the configuration files
     *                for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @return The new set of application configurations.
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}/configs")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update configuration files for an application",
            notes = "Replace the existing configuration files for application with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> updateConfigsForApplication(
            @ApiParam(
                    value = "Id of the application to update configurations for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to replace existing with.",
                    required = true
            )
            final Set<String> configs
    ) throws GenieException {
        LOG.info("Called with id " + id + " and configs " + configs);
        return this.applicationConfigService.updateConfigsForApplication(id, configs);
    }

    /**
     * Delete the all configuration files from a given application.
     *
     * @param id The id of the application to delete the configuration files
     *           from. Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/configs")
    @ApiOperation(
            value = "Remove all configuration files from an application",
            notes = "Remove all the configuration files from the application with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> removeAllConfigsForApplication(
            @ApiParam(
                    value = "Id of the application to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.applicationConfigService.removeAllConfigsForApplication(id);
    }

    /**
     * Add new jar files for a given application.
     *
     * @param id   The id of the application to add the jar file to. Not
     *             null/empty/blank.
     * @param jars The jar files to add. Not null.
     * @return The active set of application jars.
     * @throws GenieException For any error
     */
    @POST
    @Path("/{id}/jars")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new jar files to an application",
            notes = "Add the supplied jar files to the applicaiton with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> addJarsForApplication(
            @ApiParam(
                    value = "Id of the application to add jar to.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The jar files to add.",
                    required = true
            )
            final Set<String> jars
    ) throws GenieException {
        LOG.info("Called with id " + id + " and jars " + jars);
        return this.applicationConfigService.addJarsForApplication(id, jars);
    }

    /**
     * Get all the jar files for a given application.
     *
     * @param id The id of the application to get the jar files for. Not
     *           NULL/empty/blank.
     * @return The set of jar files.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/jars")
    @ApiOperation(
            value = "Get the jars for an application",
            notes = "Get the jars for the application with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getJarsForApplication(
            @ApiParam(
                    value = "Id of the application to get the jars for.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.applicationConfigService.getJarsForApplication(id);
    }

    /**
     * Update the jar files for a given application.
     *
     * @param id   The id of the application to update the jar files for. Not
     *             null/empty/blank.
     * @param jars The jar files to replace existing jar files with. Not
     *             null/empty/blank.
     * @return The active set of application jars
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}/jars")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update jar files for an application",
            notes = "Replace the existing jar files for application with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> updateJarsForApplication(
            @ApiParam(
                    value = "Id of the application to update configurations for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The jar files to replace existing with.",
                    required = true
            )
            final Set<String> jars
    ) throws GenieException {
        LOG.info("Called with id " + id + " and jars " + jars);
        return this.applicationConfigService.updateJarsForApplication(id, jars);
    }

    /**
     * Delete the all jar files from a given application.
     *
     * @param id The id of the application to delete the jar files from. Not
     *           null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/jars")
    @ApiOperation(
            value = "Remove all jar files from an application",
            notes = "Remove all the jar files from the application with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> removeAllJarsForApplication(
            @ApiParam(
                    value = "Id of the application to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.applicationConfigService.removeAllJarsForApplication(id);
    }

    /**
     * Add new tags to a given application.
     *
     * @param id   The id of the application to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @return The active tags for this application.
     * @throws GenieException For any error
     */
    @POST
    @Path("/{id}/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Add new tags to a application",
            notes = "Add the supplied tags to the application with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> addTagsForApplication(
            @ApiParam(
                    value = "Id of the application to add configuration to.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The tags to add.",
                    required = true
            )
            final Set<String> tags
    ) throws GenieException {
        LOG.info("Called with id " + id + " and config " + tags);
        return this.applicationConfigService.addTagsForApplication(id, tags);
    }

    /**
     * Get all the tags for a given application.
     *
     * @param id The id of the application to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/tags")
    @ApiOperation(
            value = "Get the tags for a application",
            notes = "Get the tags for the application with the supplied id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getTagsForApplication(
            @ApiParam(
                    value = "Id of the application to get tags for.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.applicationConfigService.getTagsForApplication(id);
    }

    /**
     * Update the tags for a given application.
     *
     * @param id   The id of the application to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @return The new set of application tags.
     * @throws GenieException For any error
     */
    @PUT
    @Path("/{id}/tags")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Update tags for a application",
            notes = "Replace the existing tags for application with given id.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> updateTagsForApplication(
            @ApiParam(
                    value = "Id of the application to update tags for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The tags to replace existing with.",
                    required = true
            )
            final Set<String> tags
    ) throws GenieException {
        LOG.info("Called with id " + id + " and tags " + tags);
        return this.applicationConfigService.updateTagsForApplication(id, tags);
    }

    /**
     * Delete the all tags from a given application.
     *
     * @param id The id of the application to delete the tags from.
     *           Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/tags")
    @ApiOperation(
            value = "Remove all tags from a application",
            notes = "Remove all the tags from the application with given id.  Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> removeAllTagsForApplication(
            @ApiParam(
                    value = "Id of the application to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id
    ) throws GenieException {
        LOG.info("Called with id " + id);
        return this.applicationConfigService.removeAllTagsForApplication(id);
    }

    /**
     * Get all the commands this application is associated with.
     *
     * @param id The id of the application to get the commands for. Not
     *           NULL/empty/blank.
     * @return The set of commands.
     * @throws GenieException For any error
     */
    @GET
    @Path("/{id}/commands")
    @ApiOperation(
            value = "Get the commands this application is associated with",
            notes = "Get the commands which this application supports.",
            response = Command.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public List<Command> getCommandsForApplication(
            @ApiParam(
                    value = "Id of the application to get the commands for.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The statuses of the commands to find.",
                    allowableValues = "ACTIVE, DEPRECATED, INACTIVE"
            )
            @QueryParam("status")
            final Set<String> statuses
    ) throws GenieException {
        LOG.info("Called with id " + id);

        Set<CommandStatus> enumStatuses = null;
        if (!statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(CommandStatus.parse(status));
                }
            }
        }
        return this.applicationConfigService.getCommandsForApplication(id, enumStatuses);
    }

    /**
     * Remove an tag from a given application.
     *
     * @param id  The id of the application to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags for the application.
     * @throws GenieException For any error
     */
    @DELETE
    @Path("/{id}/tags/{tag}")
    @ApiOperation(
            value = "Remove a tag from a application",
            notes = "Remove the given tag from the application with given id. Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted.",
            response = String.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Application not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid ID supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> removeTagForApplication(
            @ApiParam(
                    value = "Id of the application to delete from.",
                    required = true
            )
            @PathParam("id")
            final String id,
            @ApiParam(
                    value = "The tag to remove.",
                    required = true
            )
            @PathParam("tag")
            final String tag
    ) throws GenieException {
        LOG.info("Called with id " + id + " and tag " + tag);
        return this.applicationConfigService.removeTagForApplication(id, tag);
    }
}
