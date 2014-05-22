package com.netflix.genie.server.services.impl;

import java.net.HttpURLConnection;
import java.util.UUID;

import javax.persistence.EntityExistsException;
import javax.persistence.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;
import com.netflix.genie.common.model.CommandConfigElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.CommandConfigService;

/**
 * Implementation of the PersistentCommandConfig interface.
 *
 * @author amsharma
 */
public class PersistentCommandConfigImpl implements CommandConfigService {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistentCommandConfigImpl.class);

    private final PersistenceManager<CommandConfigElement> pm;

    /**
     * Default constructor.
     */
    public PersistentCommandConfigImpl() {
        // instantiate PersistenceManager
        pm = new PersistenceManager<CommandConfigElement>();
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
                        .table("CommandConfigElement");

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
                        "CommandConfigElement").clause(criteria.toString());
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

            CommandConfigElement[] elements = new CommandConfigElement[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (CommandConfigElement) results[i];
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
                CommandConfigElement element = pm.deleteEntity(id,
                        CommandConfigElement.class);

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
                    CommandConfigElement[] elements = new CommandConfigElement[]{element};
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
        CommandConfigElement commandConfigElement = request.getCommandConfig();

        // ensure that the element is not null
        if (commandConfigElement == null) {
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing commandConfig object"));
            LOG.error(ccr.getErrorMsg());
            return ccr;
        }

        // generate/validate id for request
        String id = commandConfigElement.getId();
        if (id == null) {
            if (method.equals(Verb.POST)) {
                id = UUID.randomUUID().toString();
                commandConfigElement.setId(id);
            } else {
                ccr = new CommandConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "Missing required parameter for PUT: id"));
                LOG.error(ccr.getErrorMsg());
                return ccr;
            }
        }

        // basic error checking
        String user = commandConfigElement.getUser();
        if (user == null) {
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need required param 'user' for create/update"));
            LOG.error(ccr.getErrorMsg());
            return ccr;
        }

        // ensure that the status object is valid
        String status = commandConfigElement.getStatus();
        if ((status != null) && (Types.ConfigStatus.parse(status) == null)) {
            ccr = new CommandConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Config status can only be ACTIVE, DEPRECATED, INACTIVE"));
            LOG.error(ccr.getErrorMsg());
            return ccr;
        }

        // common error checks done - set update time before proceeding
        commandConfigElement.setUpdateTime(System.currentTimeMillis());

        // handle POST and PUT differently
        if (method.equals(Verb.POST)) {
            try {
                initAndValidateNewElement(commandConfigElement);
            } catch (CloudServiceException e) {
                ccr = new CommandConfigResponse(e);
                LOG.error(ccr.getErrorMsg(), e);
                return ccr;

            }

            LOG.info("GENIE: creating config for id: " + id);
            try {
                pm.createEntity(commandConfigElement);

                // create a response
                ccr = new CommandConfigResponse();
                ccr.setMessage("Successfully created commandConfig for id: " + id);
                ccr.setCommandConfigs(new CommandConfigElement[]{commandConfigElement});
                return ccr;
            } catch (RollbackException e) {
                LOG.error(e.getMessage(), e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    ccr = new CommandConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "CommandConfig already exists for id: " + id
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
                CommandConfigElement old = pm.getEntity(id,
                        CommandConfigElement.class);
                // check if this is a create or an update
                if (old == null) {
                    try {
                        initAndValidateNewElement(commandConfigElement);
                    } catch (CloudServiceException e) {
                        ccr = new CommandConfigResponse(e);
                        LOG.error(ccr.getErrorMsg(), e);
                        return ccr;
                    }
                }
                commandConfigElement = pm.updateEntity(commandConfigElement);

                // all good - create a response
                ccr = new CommandConfigResponse();
                ccr.setMessage("Successfully updated commandConfig for id: " + id);
                ccr.setCommandConfigs(new CommandConfigElement[]{commandConfigElement});
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

    /**
     * Initialize and validate new element.
     *
     * @param commandConfigElement the element to initialize
     * @throws CloudServiceException if some params are missing - else
     * initialize, and set creation time
     */
    private void initAndValidateNewElement(CommandConfigElement commandConfigElement)
            throws CloudServiceException {

        // basic error checking
        String name = commandConfigElement.getName();
        //ArrayList<String> configs = commandConfigElement.getConfigs();

        //TODO Should we allow configs to be null?
        if ((name == null)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name}");
        } else {
            commandConfigElement.setCreateTime(commandConfigElement.getUpdateTime());
        }
    }
}
