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
import com.netflix.genie.common.model.Command;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;
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
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code for CommandConfigResource - REST end-point for supporting Command.
 *
 * @author amsharma
 * @author tgianos
 */
@Path("/v1/config/commands")
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
public class CommandConfigResourceV1 {

    private final CommandConfigService ccs;
    private static final Logger LOG = LoggerFactory
            .getLogger(CommandConfigResourceV1.class);

    /**
     * Default constructor.
     *
     * @throws CloudServiceException if there is any error
     */
    public CommandConfigResourceV1() throws CloudServiceException {
        this.ccs = ConfigServiceFactory.getCommandConfigImpl();
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
    public Command getCommandConfig(@PathParam("id") final String id) throws CloudServiceException {
        LOG.debug("Called");
        return this.ccs.getCommandConfig(id);
    }

    /**
     * Get Command configuration based on user parameters.
     *
     * @param name name for config (optional)
     * @param userName the user who created the configuration (optional)
     * @param page The page to start one (optional)
     * @param limit the max number of results to return per page (optional)
     * @return All the Commands matching the criteria or all if no criteria
     */
    @GET
    public List<Command> getCommandConfigs(
            @QueryParam("name") final String name,
            @QueryParam("userName") final String userName,
            @QueryParam("page") @DefaultValue("0") int page,
            @QueryParam("limit") @DefaultValue("1024") int limit) {
        LOG.debug("Called");
        if (page < 0) {
            page = PersistenceManager.DEFAULT_PAGE_NUMBER;
        }
        if (limit < 0) {
            limit = PersistenceManager.DEFAULT_PAGE_SIZE;
        }
        return this.ccs.getCommandConfigs(name, userName, page, limit);
    }

    /**
     * Create a Command configuration.
     *
     * @param command The command configuration to create
     * @return The command created
     * @throws CloudServiceException
     */
    @POST
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Command createCommandConfig(final Command command) throws CloudServiceException {
        LOG.debug("called to create new command configuration " + command.toString());
        return this.ccs.createCommandConfig(command);
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
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Command updateCommandConfig(
            @PathParam("id") final String id,
            final Command updateCommand) throws CloudServiceException {
        LOG.debug("Called to create/update comamnd config");
        return this.ccs.updateCommandConfig(id, updateCommand);
    }

    /**
     * Delete a command configuration.
     *
     * @param id unique id for configuration to delete
     * @return The deleted configuration
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}")
    public Command deleteCommandConfig(@PathParam("id") final String id) throws CloudServiceException {
        LOG.debug("Called");
        return this.ccs.deleteCommandConfig(id);
    }
}
