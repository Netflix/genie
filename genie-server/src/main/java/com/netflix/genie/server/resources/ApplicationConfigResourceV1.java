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
import java.util.List;
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
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
public class ApplicationConfigResourceV1 {

    private final ApplicationConfigService acs;
    private static final Logger LOG = LoggerFactory
            .getLogger(ApplicationConfigResourceV1.class);

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
    public Application getApplicationConfig(@PathParam("id") final String id)
            throws CloudServiceException {
        LOG.debug("Called");
        return this.acs.getApplicationConfig(id);
    }

    /**
     * Get Application configuration based on user parameters.
     *
     * @param name name for configuration (optional)
     * @param userName the user who created the application (optional)
     * @return All applications matching the criteria
     * @throws CloudServiceException
     */
    @GET
    public List<Application> getApplicationConfigs(
            @QueryParam("name") final String name,
            @QueryParam("userName") final String userName)
            throws CloudServiceException {
        LOG.debug("called");
        return this.acs.getApplicationConfigs(name, userName);
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
    public Application createApplicationConfig(final Application app) throws CloudServiceException {
        LOG.debug("Called to create new application");
        return this.acs.createApplicationConfig(app);
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
    public Application updateApplicationConfig(
            @PathParam("id") final String id,
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
    public Application deleteApplicationConfig(@PathParam("id") final String id)
            throws CloudServiceException {
        LOG.debug("called");
        return this.acs.deleteApplicationConfig(id);
    }
}
