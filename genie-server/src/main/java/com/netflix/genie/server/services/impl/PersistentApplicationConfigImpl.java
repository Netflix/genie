package com.netflix.genie.server.services.impl;

import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;
import com.netflix.genie.common.model.ApplicationConfig;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ApplicationConfigService;
import java.net.HttpURLConnection;
import java.util.UUID;
import javax.persistence.EntityExistsException;
import javax.persistence.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenJPA based implementation of the ApplicationConfigService.
 *
 * @author amsharma
 */
public class PersistentApplicationConfigImpl implements
        ApplicationConfigService {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistentApplicationConfigImpl.class);

    private final PersistenceManager<ApplicationConfig> pm;

    /**
     * Default constructor.
     */
    public PersistentApplicationConfigImpl() {
        // instantiate PersistenceManager
        pm = new PersistenceManager<ApplicationConfig>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationConfigResponse getApplicationConfig(String id) {
        LOG.info("called");
        return getApplicationConfig(id, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationConfigResponse getApplicationConfig(String id,
            String name) {

        LOG.info("called");
        ApplicationConfigResponse acr;

        try {
            acr = new ApplicationConfigResponse();
            Object[] results;

            if ((id == null) && (name == null)) {
                // return all
                LOG.info("GENIE: Returning all applicationConfig elements");

                // Perform a simple query for all the entities
                //TODO: Get rid of this customer query builder. Use JPA 2.0
                QueryBuilder builder = new QueryBuilder()
                        .table("ApplicationConfig");
                results = pm.query(builder);

                // set up a specific message
                acr.setMessage("Returning all applicationConfig elements");
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
                        "ApplicationConfig").clause(criteria.toString());
                results = pm.query(builder);
            }

            if (results.length == 0) {
                acr = new ApplicationConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No applicationConfigs found for input parameters"));
                LOG.error(acr.getErrorMsg());
                return acr;
            } else {
                acr.setMessage("Returning applicationConfigs for input parameters");
            }

            ApplicationConfig[] elements = new ApplicationConfig[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (ApplicationConfig) results[i];
            }
            acr.setApplicationConfigs(elements);
            return acr;
        } catch (CloudServiceException cse) {
            LOG.error(cse.getMessage(), cse);
            acr = new ApplicationConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Received exception: " + cse.getMessage()));
            return acr;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationConfigResponse createApplicationConfig(
            ApplicationConfigRequest request) {
        LOG.info("called");
        return createUpdateConfig(request, Verb.POST);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationConfigResponse updateApplicationConfig(
            ApplicationConfigRequest request) {
        LOG.info("called");
        return createUpdateConfig(request, Verb.PUT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ApplicationConfigResponse deleteApplicationConfig(String id) {

        LOG.info("called");
        ApplicationConfigResponse acr;

        if (id == null) {
            // basic error checking
            acr = new ApplicationConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: id"));
            LOG.error(acr.getErrorMsg());
            return acr;
        } else {
            LOG.info("GENIE: Deleting applicationConfig for id: " + id);

            try {
                // delete the entity
                ApplicationConfig element = pm.deleteEntity(id,
                        ApplicationConfig.class);

                if (element == null) {
                    acr = new ApplicationConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No applicationConfig exists for id: " + id));
                    LOG.error(acr.getErrorMsg());
                    return acr;
                } else {
                    // all good - create a response
                    acr = new ApplicationConfigResponse();
                    acr.setMessage("Successfully deleted applicationConfig for id: "
                            + id);
                    ApplicationConfig[] elements = new ApplicationConfig[]{element};
                    acr.setApplicationConfigs(elements);
                    return acr;
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                acr = new ApplicationConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getMessage()));
                return acr;
            }
        }
    }

    /**
     * Common private method called by the create and update Can use either
     * method to create/update resource.
     */
    private ApplicationConfigResponse createUpdateConfig(ApplicationConfigRequest request,
            Verb method) {
        LOG.info("called");
        ApplicationConfigResponse acr;
        ApplicationConfig applicationConfig = request.getApplicationConfig();

        // ensure that the element is not null
        if (applicationConfig == null) {
            acr = new ApplicationConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing applicationConfig object"));
            LOG.error(acr.getErrorMsg());
            return acr;
        }

        // generate/validate id for request
        String id = applicationConfig.getId();
        if (id == null) {
            if (method.equals(Verb.POST)) {
                id = UUID.randomUUID().toString();
                applicationConfig.setId(id);
            } else {
                acr = new ApplicationConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "Missing required parameter for PUT: id"));
                LOG.error(acr.getErrorMsg());
                return acr;
            }
        }

        // basic error checking
        String user = applicationConfig.getUser();
        if (user == null) {
            acr = new ApplicationConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need required param 'user' for create/update"));
            LOG.error(acr.getErrorMsg());
            return acr;
        }

        // ensure that the status object is valid
        String status = applicationConfig.getStatus();
        if ((status != null) && (Types.ConfigStatus.parse(status) == null)) {
            acr = new ApplicationConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Config status can only be ACTIVE, DEPRECATED, INACTIVE"));
            LOG.error(acr.getErrorMsg());
            return acr;
        }

        // common error checks done - set update time before proceeding
        applicationConfig.setUpdateTime(System.currentTimeMillis());

        // handle POST and PUT differently
        if (method.equals(Verb.POST)) {
            try {
                initAndValidateNewElement(applicationConfig);
            } catch (CloudServiceException e) {
                acr = new ApplicationConfigResponse(e);
                LOG.error(acr.getErrorMsg(), e);
                return acr;

            }

            LOG.info("GENIE: creating config for id: " + id);
            try {
                pm.createEntity(applicationConfig);

                // create a response
                acr = new ApplicationConfigResponse();
                acr.setMessage("Successfully created applicationConfig for id: " + id);
                acr.setApplicationConfigs(new ApplicationConfig[]{applicationConfig});
                return acr;
            } catch (RollbackException e) {
                LOG.error(e.getMessage(), e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    acr = new ApplicationConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "ApplicationConfig already exists for id: " + id
                            + ", use PUT to update config"));
                    return acr;
                } else {
                    // unknown exception - send it back
                    acr = new ApplicationConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "Received exception: " + e.getCause()));
                    LOG.error(acr.getErrorMsg());
                    return acr;
                }
            }
        } else {
            // method is PUT
            LOG.info("GENIE: updating config for id: " + id);

            try {
                ApplicationConfig old = pm.getEntity(id,
                        ApplicationConfig.class);
                // check if this is a create or an update
                if (old == null) {
                    try {
                        initAndValidateNewElement(applicationConfig);
                    } catch (CloudServiceException e) {
                        acr = new ApplicationConfigResponse(e);
                        LOG.error(acr.getErrorMsg(), e);
                        return acr;
                    }
                }
                applicationConfig = pm.updateEntity(applicationConfig);

                // all good - create a response
                acr = new ApplicationConfigResponse();
                acr.setMessage("Successfully updated applicationConfig for id: " + id);
                acr.setApplicationConfigs(new ApplicationConfig[]{applicationConfig});
                return acr;
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                // unknown exception - send it back
                acr = new ApplicationConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getCause()));
                return acr;
            }
        }
    }

    /**
     * Initialize and validate new element.
     *
     * @param applicationConfigElement the element to initialize
     * @throws CloudServiceException if some params applicationConfig missing - else
     * initialize, and set creation time
     */
    private void initAndValidateNewElement(ApplicationConfig applicationConfig)
            throws CloudServiceException {

        // basic error checking
        String name = applicationConfig.getName();
        //ArrayList<String> configs = applicationConfigElement.getConfigs();
        //ArrayList<String> jars = applicationConfigElement.getJars();

        //TODO Should we allow configs and jars to be null?
        if ((name == null)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name}");
        } else {
            applicationConfig.setCreateTime(applicationConfig.getUpdateTime());
        }
    }
}
