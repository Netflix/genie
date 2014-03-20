package com.netflix.genie.server.services.impl;

import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;
import com.netflix.genie.common.model.ApplicationConfigElement;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ApplicationConfigService;

/**
 * OpenJPA based implementation of the ApplicationConfigService.
 * @author amsharma
 */
public class PersistentApplicationConfigImpl implements
        ApplicationConfigService {

    private static Logger logger = LoggerFactory
            .getLogger(PersistentApplicationConfigImpl.class);
    
    private PersistenceManager<ApplicationConfigElement> pm;
    
    /**
     * Default constructor.
     */
    public PersistentApplicationConfigImpl() {
        // instantiate PersistenceManager
        pm = new PersistenceManager<ApplicationConfigElement>();
    }
    
    /** {@inheritDoc} */
    @Override
    public ApplicationConfigResponse getApplicationConfig(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ApplicationConfigResponse getApplicationConfig(String id,
            String name) {
    
        logger.info("called");
        ApplicationConfigResponse acr = null;

        try {
            acr = new ApplicationConfigResponse();
            Object[] results;

            if ((id == null) && (name == null)) {
                // return all
                logger.info("GENIE: Returning all applicationConfig elements");

                // Perform a simple query for all the entities
                QueryBuilder builder = new QueryBuilder()
                        .table("ApplicationConfigElement");
                results = pm.query(builder);

                // set up a specific message
                acr.setMessage("Returning all applicationConfig elements");
            } else {
                // do some filtering
                logger.info("GENIE: Returning config for {id, name}: "
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
                        "ApplicationElement").clause(criteria.toString());
                results = pm.query(builder);
            }

            if (results.length == 0) {
                acr = new ApplicationConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No applicationConfigs found for input parameters"));
                logger.error(acr.getErrorMsg());
                return acr;
            } else {
                acr.setMessage("Returning applicationConfigs for input parameters");
            }

            ApplicationConfigElement[] elements = new ApplicationConfigElement[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (ApplicationConfigElement) results[i];
            }
            acr.setApplicationConfigs(elements);
            return acr;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            acr = new ApplicationConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Received exception: " + e.getMessage()));
            return acr;
        }
    }

    /** {@inheritDoc} */
    @Override
    public ApplicationConfigResponse createApplicationConfig(
            ApplicationConfigRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ApplicationConfigResponse updateApplicationConfig(
            ApplicationConfigRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ApplicationConfigResponse deleteApplicationConfig(String id) {
        // TODO Auto-generated method stub
        return null;
    }
}
