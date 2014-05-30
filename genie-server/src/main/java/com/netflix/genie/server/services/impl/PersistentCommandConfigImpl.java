package com.netflix.genie.server.services.impl;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.UUID;

import javax.persistence.EntityExistsException;
import javax.persistence.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;

import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.util.ResponseUtil;
import org.apache.commons.lang.StringUtils;

/**
 * Implementation of the PersistentCommandConfig interface.
 *
 * @author amsharma
 */
public class PersistentCommandConfigImpl implements CommandConfigService {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistentCommandConfigImpl.class);

    private final PersistenceManager<Command> pm;

    /**
     * Default constructor.
     */
    public PersistentCommandConfigImpl() {
        // instantiate PersistenceManager
        pm = new PersistenceManager<Command>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CommandConfigResponse getCommandConfig(String id) {
        LOG.info("called");
        return getCommandConfig(id, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CommandConfigResponse getCommandConfig(String id, String name) {
        LOG.info("called");
        CommandConfigResponse ccr = null;

        try {
            ccr = new CommandConfigResponse();
            Object[] results;

            if ((id == null) && (name == null)) {
                // return all
                LOG.info("GENIE: Returning all commandConfig elements");

                // Perform a simple query for all the entities
                QueryBuilder builder = new QueryBuilder()
                        .table("Command");

                results = pm.query(builder);

                // set up a specific message
                ccr.setMessage("Returning all commandConfig elements");
            } else {
                // do some filtering
                LOG.info("GENIE: Returning config for {id, name}: "
                        + "{" + id + ", " + name + "}");

                // construct query
                ClauseBuilder criteria = new ClauseBuilder(ClauseBuilder.AND);
                if (id != null) {
                    criteria.append("id = '" + id + "'");
                }
                if (name != null) {
                    criteria.append("name = '" + name + "'");
                }

                // Get all the results as an array
                QueryBuilder builder = new QueryBuilder().table(
                        "Command").clause(criteria.toString());
                results = pm.query(builder);
            }

            if (results.length == 0) {
                ccr = new CommandConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No commandConfigs found for input parameters"));
                LOG.error(ccr.getErrorMsg());
                return ccr;
            } else {
                ccr.setMessage("Returning commandConfigs for input parameters");
            }

            Command[] elements = new Command[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (Command) results[i];
            }
            ccr.setCommandConfigs(elements);
            return ccr;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Received exception: " + e.getMessage()));
            return ccr;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CommandConfigResponse createCommandConfig(
            CommandConfigRequest request) {
        LOG.info("called");
        return createUpdateConfig(request, Verb.POST);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CommandConfigResponse updateCommandConfig(
            CommandConfigRequest request) {
        LOG.info("called");
        return createUpdateConfig(request, Verb.PUT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CommandConfigResponse deleteCommandConfig(String id) {

        LOG.info("called");
        CommandConfigResponse ccr = null;

        if (id == null) {
            // basic error checking
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: id"));
            LOG.error(ccr.getErrorMsg());
            return ccr;
        } else {
            LOG.info("GENIE: Deleting commandConfig for id: " + id);

            try {
                // delete the entity
                Command element = pm.deleteEntity(id,
                        Command.class);

                if (element == null) {
                    ccr = new CommandConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No commandConfig exists for id: " + id));
                    LOG.error(ccr.getErrorMsg());
                    return ccr;
                } else {
                    // all good - create a response
                    ccr = new CommandConfigResponse();
                    ccr.setMessage("Successfully deleted commandConfig for id: "
                            + id);
                    Command[] elements = new Command[]{element};
                    ccr.setCommandConfigs(elements);
                    return ccr;
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                ccr = new CommandConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getMessage()));
                return ccr;
            }
        }
    }

    /**
     * Common private method called by the create and update Can use either
     * method to create/update resource.
     */
    private CommandConfigResponse createUpdateConfig(CommandConfigRequest request,
            Verb method) {
        LOG.info("called");
        CommandConfigResponse ccr = null;
        Command commandConfig = request.getCommandConfig();

        // ensure that the element is not null
        if (commandConfig == null) {
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing commandConfig object"));
            LOG.error(ccr.getErrorMsg());
            return ccr;
        }

        // generate/validate id for request
        String id = commandConfig.getId();
        if (id == null) {
            if (method.equals(Verb.POST)) {
                id = UUID.randomUUID().toString();
                commandConfig.setId(id);
            } else {
                ccr = new CommandConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "Missing required parameter for PUT: id"));
                LOG.error(ccr.getErrorMsg());
                return ccr;
            }
        }

        // basic error checking
        String user = commandConfig.getUser();
        if (user == null) {
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need required param 'user' for create/update"));
            LOG.error(ccr.getErrorMsg());
            return ccr;
        }

        // ensure that child command configs exist
        try {
            validateChildren(commandConfig);
        } catch (CloudServiceException cse) {
            ccr = new CommandConfigResponse(cse);
            LOG.error(ccr.getErrorMsg(), cse);
            return ccr;
        }
        
        // common error checks done - set update time before proceeding
        //Should now be done automatically by @PreUpdate but will leave just in case
//        commandConfig.setUpdateTime(System.currentTimeMillis());

        // handle POST and PUT differently
        if (method.equals(Verb.POST)) {
            try {
                initAndValidateNewElement(commandConfig);
            } catch (CloudServiceException e) {
                ccr = new CommandConfigResponse(e);
                LOG.error(ccr.getErrorMsg(), e);
                return ccr;

            }

            LOG.info("GENIE: creating config for id: " + id);
            try {
                pm.createEntity(commandConfig);

                // create a response
                ccr = new CommandConfigResponse();
                ccr.setMessage("Successfully created commandConfig for id: " + id);
                ccr.setCommandConfigs(new Command[]{commandConfig});
                return ccr;
            } catch (RollbackException e) {
                LOG.error(e.getMessage(), e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    ccr = new CommandConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "Command already exists for id: " + id
                            + ", use PUT to update config"));
                    return ccr;
                } else {
                    // unknown exception - send it back
                    ccr = new CommandConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "Received exception: " + e.getCause()));
                    LOG.error(ccr.getErrorMsg());
                    return ccr;
                }
            }
        } else {
            // method is PUT
            LOG.info("GENIE: updating config for id: " + id);

            try {
                Command old = pm.getEntity(id,
                        Command.class);
                // check if this is a create or an update
                if (old == null) {
                    try {
                        initAndValidateNewElement(commandConfig);
                    } catch (CloudServiceException e) {
                        ccr = new CommandConfigResponse(e);
                        LOG.error(ccr.getErrorMsg(), e);
                        return ccr;
                    }
                }
                commandConfig = pm.updateEntity(commandConfig);

                // all good - create a response
                ccr = new CommandConfigResponse();
                ccr.setMessage("Successfully updated commandConfig for id: " + id);
                ccr.setCommandConfigs(new Command[]{commandConfig});
                return ccr;
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                // unknown exception - send it back
                ccr = new CommandConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getCause()));
                return ccr;
            }
        }
    }

    private void validateChildren(Command commandConfig) 
            throws CloudServiceException {

        ArrayList<String> appIds = commandConfig.getAppIds();
        PersistenceManager<Application> pma = new PersistenceManager<Application>();
        ArrayList<Application> appList = new ArrayList<Application>();
        
        for (String appId: appIds) {
            Application ae = (Application) pma.getEntity(appId, Application.class);
            if (ae != null) {
                appList.add(ae);
            } else {
                throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                        "Application Does Not Exist: {" + appId + "}");
            }
        }
        commandConfig.setApplications(appList);
    }

    /**
     * Initialize and validate new element.
     *
     * @param commandConfig the element to initialize
     * @throws CloudServiceException if some params commandConfig are missing - else
     * initialize, and set creation time
     */
    private void initAndValidateNewElement(Command commandConfig)
            throws CloudServiceException {

        // basic error checking
        String name = commandConfig.getName();
        //ArrayList<String> configs = commandConfigElement.getConfigs();

        //TODO Should we allow configs to be null?
        if (StringUtils.isEmpty(name)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name}");
        }
    }
}
