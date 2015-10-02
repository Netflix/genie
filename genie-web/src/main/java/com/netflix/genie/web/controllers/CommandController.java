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
package com.netflix.genie.web.controllers;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.web.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ResponseHeader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * REST end-point for supporting commands.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3/commands")
@Api(value = "commands", tags = "commands", description = "Manage the available commands")
public class CommandController {

    private static final Logger LOG = LoggerFactory.getLogger(CommandController.class);

    private final CommandService commandService;
    private final CommandResourceAssembler commandResourceAssembler;
    private final ApplicationResourceAssembler applicationResourceAssembler;
    private final ClusterResourceAssembler clusterResourceAssembler;

    /**
     * Constructor.
     *
     * @param commandService               The command configuration service to use.
     * @param commandResourceAssembler     The assembler to use to convert commands to command HAL resources
     * @param applicationResourceAssembler The assembler to use to convert applicaitons to application HAL resources
     * @param clusterResourceAssembler     The assembler to use to convert clusters to cluster HAL resources
     */
    @Autowired
    public CommandController(
            final CommandService commandService,
            final CommandResourceAssembler commandResourceAssembler,
            final ApplicationResourceAssembler applicationResourceAssembler,
            final ClusterResourceAssembler clusterResourceAssembler
    ) {
        this.commandService = commandService;
        this.commandResourceAssembler = commandResourceAssembler;
        this.applicationResourceAssembler = applicationResourceAssembler;
        this.clusterResourceAssembler = clusterResourceAssembler;
    }

    /**
     * Create a Command configuration.
     *
     * @param command The command configuration to create
     * @return The command created
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(
            value = "Create a command",
            notes = "Create a command from the supplied information."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CREATED,
                    message = "Successfully created the command",
                    responseHeaders = {@ResponseHeader(name = HttpHeaders.LOCATION, response = String.class)}
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CONFLICT,
                    message = "A command with the supplied id already exists"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public ResponseEntity<Void> createCommand(
            @ApiParam(
                    value = "The command to create.",
                    required = true
            )
            @RequestBody
            final Command command
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called to create new command configuration " + command.toString());
        }
        final String id = this.commandService.createCommand(command);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
                ServletUriComponentsBuilder
                        .fromCurrentRequest()
                        .path("/{id}")
                        .buildAndExpand(id)
                        .toUri()
        );
        return new ResponseEntity<>(httpHeaders, HttpStatus.CREATED);
    }

    /**
     * Get Command configuration for given id.
     *
     * @param id unique id for command configuration
     * @return The command configuration
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Find a command by id",
            notes = "Get the command by id if it exists",
            response = CommandResource.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public CommandResource getCommand(
            @ApiParam(
                    value = "Id of the command to get.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to get command with id " + id);
        }
        return this.commandResourceAssembler.toResource(this.commandService.getCommand(id));
    }

    /**
     * Get Command configuration based on user parameters.
     *
     * @param name      Name for command (optional)
     * @param userName  The user who created the configuration (optional)
     * @param statuses  The statuses of the commands to get (optional)
     * @param tags      The set of tags you want the command for.
     * @param page      The page to get
     * @param assembler The paged resources assembler to use
     * @return All the Commands matching the criteria or all if no criteria
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Find commands",
            notes = "Find commands by the submitted criteria.",
            response = CommandResource.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "One of the statuses was invalid"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public PagedResources<CommandResource> getCommands(
            @ApiParam(
                    value = "Name of the command."
            )
            @RequestParam(value = "name", required = false)
            final String name,
            @ApiParam(
                    value = "User who created the command."
            )
            @RequestParam(value = "userName", required = false)
            final String userName,
            @ApiParam(
                    value = "The statuses of the commands to find.",
                    allowableValues = "ACTIVE, DEPRECATED, INACTIVE"
            )
            @RequestParam(value = "status", required = false)
            final Set<String> statuses,
            @ApiParam(
                    value = "Tags for the cluster."
            )
            @RequestParam(value = "tag", required = false)
            final Set<String> tags,
            @PageableDefault(page = 0, size = 64, sort = {"updated"}, direction = Sort.Direction.DESC)
            final Pageable page,
            final PagedResourcesAssembler<Command> assembler
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Called [name | userName | status | tags | page]"
            );
            LOG.debug(
                    name
                            + " | "
                            + userName
                            + " | "
                            + statuses
                            + " | "
                            + tags
                            + " | "
                            + page
            );
        }

        Set<CommandStatus> enumStatuses = null;
        if (statuses != null && !statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(CommandStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(CommandStatus.parse(status));
                }
            }
        }
        return assembler.toResource(
                this.commandService.getCommands(name, userName, enumStatuses, tags, page),
                this.commandResourceAssembler
        );
    }

    /**
     * Update command configuration.
     *
     * @param id            unique id for the configuration to update.
     * @param updateCommand the information to update the command with
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update a command",
            notes = "Update a command from the supplied information."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully updated"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command to update not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void updateCommand(
            @ApiParam(
                    value = "Id of the command to update.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The command information to update.",
                    required = true
            )
            @RequestBody
            final Command updateCommand
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to update command");
        }
        this.commandService.updateCommand(id, updateCommand);
    }

    /**
     * Delete all applications from database.
     *
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Delete all commands",
            notes = "Delete all available commands and get them back."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void deleteAllCommands() throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called to delete all commands.");
        }
        this.commandService.deleteAllCommands();
    }

    /**
     * Delete a command.
     *
     * @param id unique id for configuration to delete
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Delete a command",
            notes = "Delete a command with the supplied id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void deleteCommand(
            @ApiParam(
                    value = "Id of the command to delete.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to delete command with id " + id);
        }
        this.commandService.deleteCommand(id);
    }

    /**
     * Add new configuration files to a given command.
     *
     * @param id      The id of the command to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Add new configuration files to a command",
            notes = "Add the supplied configuration files to the command with the supplied id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully added"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void addConfigsForCommand(
            @ApiParam(
                    value = "Id of the command to add configuration to.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to add.",
                    required = true
            )
            @RequestBody
            final Set<String> configs
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and config " + configs);
        }
        this.commandService.addConfigsForCommand(id, configs);
    }

    /**
     * Get all the configuration files for a given command.
     *
     * @param id The id of the command to get the configuration files for. Not
     *           NULL/empty/blank.
     * @return The active set of configuration files.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Get the configuration files for a command",
            notes = "Get the configuration files for the command with the supplied id.",
            response = String.class,
            responseContainer = "Set"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getConfigsForCommand(
            @ApiParam(
                    value = "Id of the command to get configurations for.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.commandService.getConfigsForCommand(id);
    }

    /**
     * Update the configuration files for a given command.
     *
     * @param id      The id of the command to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update configuration files for an command",
            notes = "Replace the existing configuration files for command with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully updated"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void updateConfigsForCommand(
            @ApiParam(
                    value = "Id of the command to update configurations for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The configuration files to replace existing with.",
                    required = true
            )
            @RequestBody
            final Set<String> configs
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and configs " + configs);
        }
        this.commandService.updateConfigsForCommand(id, configs);
    }

    /**
     * Delete the all configuration files from a given command.
     *
     * @param id The id of the command to delete the configuration files from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/configs", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove all configuration files from a command",
            notes = "Remove all the configuration files from the command with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void removeAllConfigsForCommand(
            @ApiParam(
                    value = "Id of the command to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        this.commandService.removeAllConfigsForCommand(id);
    }

    /**
     * Add new tags to a given command.
     *
     * @param id   The id of the command to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Add new tags to a command",
            notes = "Add the supplied tags to the command with the supplied id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully added"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void addTagsForCommand(
            @ApiParam(
                    value = "Id of the command to add configuration to.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The tags to add.",
                    required = true
            )
            @RequestBody
            final Set<String> tags
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and tags " + tags);
        }
        this.commandService.addTagsForCommand(id, tags);
    }

    /**
     * Get all the tags for a given command.
     *
     * @param id The id of the command to get the tags for. Not
     *           NULL/empty/blank.
     * @return The active set of tags.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Get the tags for a command",
            notes = "Get the tags for the command with the supplied id.",
            response = String.class,
            responseContainer = "Set"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<String> getTagsForCommand(
            @ApiParam(
                    value = "Id of the command to get tags for.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.commandService.getTagsForCommand(id);
    }

    /**
     * Update the tags for a given command.
     *
     * @param id   The id of the command to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing configuration
     *             files with. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Update tags for a command",
            notes = "Replace the existing tags for command with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully updated"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void updateTagsForCommand(
            @ApiParam(
                    value = "Id of the command to update tags for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The tags to replace existing with.",
                    required = true
            )
            @RequestBody
            final Set<String> tags
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and tags " + tags);
        }
        this.commandService.updateTagsForCommand(id, tags);
    }

    /**
     * Delete the all tags from a given command.
     *
     * @param id The id of the command to delete the tags from.
     *           Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove all tags from a command",
            notes = "Remove all the tags from the command with given id.  Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void removeAllTagsForCommand(
            @ApiParam(
                    value = "Id of the command to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        this.commandService.removeAllTagsForCommand(id);
    }

    /**
     * Remove an tag from a given command.
     *
     * @param id  The id of the command to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/tags/{tag}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove a tag from a command",
            notes = "Remove the given tag from the command with given id.  Note that the genie name space tags"
                    + "prefixed with genie.id and genie.name cannot be deleted."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void removeTagForCommand(
            @ApiParam(
                    value = "Id of the command to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The tag to remove.",
                    required = true
            )
            @PathVariable("tag")
            final String tag
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and tag " + tag);
        }
        this.commandService.removeTagForCommand(id, tag);
    }

    /**
     * Add applications for the given command.
     *
     * @param id             The id of the command to add the applications to. Not
     *                       null/empty/blank.
     * @param applicationIds The ids of the applications to add. Not null.
     * @throws GenieException For any error
     */
    @RequestMapping(
            value = "/{id}/applications", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Add applications for a command",
            notes = "Add the supplied applications to the command "
                    + "with the supplied id. Applications should already "
                    + "have been created."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully added"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void addApplicationsForCommand(
            @ApiParam(
                    value = "Id of the command to set application for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The ids of the applications to set.",
                    required = true
            )
            @RequestBody
            final Set<String> applicationIds
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.info("Called with id " + id + " and application " + applicationIds);
        }
        this.commandService.addApplicationsForCommand(id, applicationIds);
    }

    /**
     * Get the applications configured for a given command.
     *
     * @param id The id of the command to get the application files for. Not
     *           NULL/empty/blank.
     * @return The active applications for the command.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/applications", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Get the applications for a command",
            notes = "Get the applications for the command with the supplied id.",
            response = Application.class,
            responseContainer = "Set"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<ApplicationResource> getApplicationsForCommand(
            @ApiParam(
                    value = "Id of the command to get the application for.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.commandService.getApplicationsForCommand(id)
                .stream()
                .map(this.applicationResourceAssembler::toResource)
                .collect(Collectors.toSet());
    }

    /**
     * Set the applications for the given command.
     *
     * @param id             The id of the command to add the applications to. Not
     *                       null/empty/blank.
     * @param applicationIds The ids of the applications to set. Not null.
     * @throws GenieException For any error
     */
    @RequestMapping(
            value = "/{id}/applications", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE
    )
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Set applications for a command",
            notes = "Set the supplied applications to the command "
                    + "with the supplied id. Applications should already "
                    + "have been created. Replaces existing applications."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully updated"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void setApplicationsForCommand(
            @ApiParam(
                    value = "Id of the command to set application for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "The ids of the applications to set.",
                    required = true
            )
            @RequestBody
            final Set<String> applicationIds
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.info("Called with id " + id + " and application " + applicationIds);
        }
        this.commandService.setApplicationsForCommand(id, applicationIds);
    }

    /**
     * Remove the applications from a given command.
     *
     * @param id The id of the command to delete the applications from. Not
     *           null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/applications", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove applications from a command",
            notes = "Remove the applications from the command with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void removeAllApplicationsForCommand(
            @ApiParam(
                    value = "Id of the command to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id '" + id + "'.");
        }
        this.commandService.removeApplicationsForCommand(id);
    }

    /**
     * Remove the application from a given command.
     *
     * @param id    The id of the command to delete the application from. Not
     *              null/empty/blank.
     * @param appId The id of the application to remove from the command. Not null/empty/blank.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/applications/{appId}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @ApiOperation(
            value = "Remove applications from a command",
            notes = "Remove the applications from the command with given id."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NO_CONTENT,
                    message = "Successfully deleted"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void removeApplicationForCommand(
            @ApiParam(
                    value = "Id of the command to delete from.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "Id of the application to remove.",
                    required = true
            )
            @PathVariable("appId")
            final String appId
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id '" + id + "' and app id " + appId);
        }
        this.commandService.removeApplicationForCommand(id, appId);
    }

    /**
     * Get all the clusters this command is associated with.
     *
     * @param id       The id of the command to get the clusters for. Not
     *                 NULL/empty/blank.
     * @param statuses The statuses of the clusters to get
     * @return The list of clusters.
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}/clusters", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Get the clusters this command is associated with",
            notes = "Get the clusters which this command exists on supports.",
            response = Cluster.class,
            responseContainer = "Set"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Command not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid required parameter supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public Set<ClusterResource> getClustersForCommand(
            @ApiParam(
                    value = "Id of the command to get the clusters for.",
                    required = true
            )
            @PathVariable("id")
            final String id,
            @ApiParam(
                    value = "Status of the cluster.",
                    allowableValues = "UP, OUT_OF_SERVICE, TERMINATED"
            )
            @RequestParam(value = "status", required = false)
            final Set<String> statuses
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id + " and statuses " + statuses);
        }

        Set<ClusterStatus> enumStatuses = null;
        if (statuses != null && !statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(ClusterStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(ClusterStatus.parse(status));
                }
            }
        }

        return this.commandService.getClustersForCommand(id, enumStatuses)
                .stream()
                .map(this.clusterResourceAssembler::toResource)
                .collect(Collectors.toSet());
    }
}
