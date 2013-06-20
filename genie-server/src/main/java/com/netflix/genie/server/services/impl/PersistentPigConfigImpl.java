/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.server.services.impl;

import java.net.HttpURLConnection;
import java.util.UUID;

import javax.persistence.EntityExistsException;
import javax.persistence.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.PigConfigRequest;
import com.netflix.genie.common.messages.PigConfigResponse;
import com.netflix.genie.common.model.PigConfigElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.PigConfigService;
import com.netflix.niws.client.http.HttpClientRequest.Verb;

/**
 * OpenJPA based implementation of the PigConfigService.
 *
 * @author skrishnan
 */
public class PersistentPigConfigImpl implements PigConfigService {

    private static Logger logger = LoggerFactory
            .getLogger(PersistentPigConfigImpl.class);

    private PersistenceManager<PigConfigElement> pm;

    /**
     * Default constructor.
     */
    public PersistentPigConfigImpl() {
        // instantiate PersistenceManager
        pm = new PersistenceManager<PigConfigElement>();
    }

    /** {@inheritDoc} */
    @Override
    public PigConfigResponse getPigConfig(String id) {
        logger.info("called");
        return getPigConfig(id, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public PigConfigResponse getPigConfig(String id, String name, String type) {
        logger.info("called");
        PigConfigResponse pcr = null;

        try {
            pcr = new PigConfigResponse();
            Object[] results;

            if ((id == null) && (name == null) && (type == null)) {
                // return all
                logger.info("GENIE: Returning all pigConfig elements");

                // Perform a simple query for all the PigConfigElement entities
                QueryBuilder builder = new QueryBuilder()
                        .table("PigConfigElement");
                results = pm.query(builder);

                // set up a specific message
                pcr.setMessage("Returning all pigConfig elements");
            } else {
                // do some filtering
                logger.info("GENIE: Returning pigConfig for {id, name, type}: "
                        + "{" + id + ", " + name + ", " + type + "}");

                // construct query
                ClauseBuilder criteria = new ClauseBuilder(ClauseBuilder.AND);
                if (id != null) {
                    criteria.append("id = '" + id + "'");
                }
                if (name != null) {
                    criteria.append("name = '" + name + "'");
                }
                if (type != null) {
                    if (Types.Configuration.parse(type) == null) {
                        pcr = new PigConfigResponse(new CloudServiceException(
                                HttpURLConnection.HTTP_BAD_REQUEST,
                                "Type can only be PROD, TEST or UNITTEST"));
                        return pcr;
                    }
                    criteria.append("type = '" + type.toUpperCase() + "'");
                }

                // Get all the results as an array
                QueryBuilder builder = new QueryBuilder().table(
                        "PigConfigElement").clause(criteria.toString());
                results = pm.query(builder);
            }

            if (results.length == 0) {
                pcr = new PigConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No pigConfigs found for input parameters"));
                logger.error(pcr.getErrorMsg());
                return pcr;
            } else {
                pcr.setMessage("Returning pigConfigs for input parameters");
            }

            PigConfigElement[] elements = new PigConfigElement[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (PigConfigElement) results[i];
            }
            pcr.setPigConfigs(elements);
            return pcr;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            pcr = new PigConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Received exception: " + e.getMessage()));
            return pcr;
        }
    }

    /** {@inheritDoc} */
    @Override
    public PigConfigResponse createPigConfig(PigConfigRequest request) {
        logger.info("called");
        return createUpdateConfig(request, Verb.POST);
    }

    /** {@inheritDoc} */
    @Override
    public PigConfigResponse updatePigConfig(PigConfigRequest request) {
        logger.info("called");
        return createUpdateConfig(request, Verb.PUT);
    }

    /** {@inheritDoc} */
    @Override
    public PigConfigResponse deletePigConfig(String id) {
        logger.info("called");
        PigConfigResponse pcr = null;

        if (id == null) {
            // basic error checking
            pcr = new PigConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: id"));
            logger.error(pcr.getErrorMsg());
            return pcr;
        } else {
            // do some filtering
            logger.info("GENIE: Deleting pigConfig for id: " + id);

            try {
                // delete the entity
                PigConfigElement element = pm.deleteEntity(id,
                        PigConfigElement.class);

                if (element == null) {
                    pcr = new PigConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No pigConfig exists for id: " + id));
                    logger.error(pcr.getErrorMsg());
                    return pcr;
                } else {
                    // all good - create a response
                    pcr = new PigConfigResponse();
                    pcr.setMessage("Successfully deleted pigConfig for id: "
                            + id);
                    PigConfigElement[] elements = new PigConfigElement[] {element};
                    pcr.setPigConfigs(elements);
                    return pcr;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                pcr = new PigConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getMessage()));
                return pcr;
            }
        }
    }

    /*
     * Common private method called by the create and update Can use either
     * method to create/update resource.
     */
    private PigConfigResponse createUpdateConfig(PigConfigRequest request,
            Verb method) {
        logger.debug("called");
        PigConfigResponse pcr = null;
        PigConfigElement pigConfigElement = request.getPigConfig();
        // ensure that the element is not null
        if (pigConfigElement == null) {
            pcr = new PigConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing pigConfig object"));
            logger.error(pcr.getErrorMsg());
            return pcr;
        }

        // generate/validate id for request
        String id = pigConfigElement.getId();
        if (id == null) {
            if (method.equals(Verb.POST)) {
                // create UUID for POST, if it doesn't exist
                id = UUID.randomUUID().toString();
                pigConfigElement.setId(id);
            } else {
                pcr = new PigConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "Missing required parameter for PUT: id"));
                logger.error(pcr.getErrorMsg());
                return pcr;
            }
        }

        // basic error checking
        String user = pigConfigElement.getUser();
        if (user == null) {
            pcr = new PigConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need required param 'user' for create/update"));
            logger.error(pcr.getErrorMsg());
            return pcr;
        }

        // ensure that the type is valid
        String type = pigConfigElement.getType();
        if ((type != null) && (Types.Configuration.parse(type) == null)) {
            pcr = new PigConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Invalid type - must be one of PROD, TEST or UNITTEST"));
            logger.error(pcr.getErrorMsg());
            return pcr;
        }

        // ensure that the status object is valid
        String status = pigConfigElement.getStatus();
        if ((status != null) && (Types.ConfigStatus.parse(status) == null)) {
            pcr = new PigConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Config status can only be ACTIVE, DEPRECATED, INACTIVE"));
            logger.error(pcr.getErrorMsg());
            return pcr;
        }

        // common error checks done - set update time before proceeding
        pigConfigElement.setUpdateTime(System.currentTimeMillis());

        // handle POST and PUT differently
        if (method.equals(Verb.POST)) {
            try {
                initAndValidateNewElement(pigConfigElement);
            } catch (CloudServiceException e) {
                pcr = new PigConfigResponse(e);
                logger.error(pcr.getErrorMsg(), e);
                return pcr;
            }

            logger.info("GENIE: creating pigConfig for id: " + id);
            try {
                pm.createEntity(pigConfigElement);

                // create a response
                pcr = new PigConfigResponse();
                pcr.setMessage("Successfully created pigConfig for id: " + id);
                pcr.setPigConfigs(new PigConfigElement[] {pigConfigElement});
                return pcr;
            } catch (RollbackException e) {
                logger.error(e.getMessage(), e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    pcr = new PigConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "PigConfig already exists for id: " + id
                                    + ", use PUT to update pigConfig"));
                    logger.error(pcr.getErrorMsg());
                    return pcr;
                } else {
                    // unknown exception - send it back
                    pcr = new PigConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "Received exception: " + e.getCause()));
                    logger.error(pcr.getErrorMsg());
                    return pcr;
                }
            }
        } else {
            // method is PUT
            logger.info("GENIE: updating pigConfig for id: " + id);

            try {
                PigConfigElement old = pm.getEntity(id, PigConfigElement.class);
                // check if this is a create or an update
                if (old == null) {
                    try {
                        initAndValidateNewElement(pigConfigElement);
                    } catch (CloudServiceException e) {
                        pcr = new PigConfigResponse(e);
                        logger.error(pcr.getErrorMsg(), e);
                        return pcr;
                    }
                }
                pigConfigElement = pm.updateEntity(pigConfigElement);

                // all good - create a response
                pcr = new PigConfigResponse();
                pcr.setMessage("Successfully updated pigConfig for id: " + id);
                pcr.setPigConfigs(new PigConfigElement[] {pigConfigElement});
                return pcr;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                // unknown exception - send it back
                pcr = new PigConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getCause()));
                return pcr;
            }
        }
    }

    /*
     * Throws Exception if some params are missing - else initialize, and set
     * creation time.
     */
    private void initAndValidateNewElement(PigConfigElement pigConfigElement)
            throws CloudServiceException {
        String name = pigConfigElement.getName();
        String s3PigProps = pigConfigElement.getS3PigProperties();
        Types.Configuration type = Types.Configuration.parse(pigConfigElement
                .getType());

        if ((name == null) || (s3PigProps == null) || (type == null)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name, s3PigProps, type}");
        } else {
            pigConfigElement.setCreateTime(pigConfigElement.getUpdateTime());
        }
    }
}
