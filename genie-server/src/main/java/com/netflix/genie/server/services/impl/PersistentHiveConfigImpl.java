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
import com.netflix.genie.common.messages.HiveConfigRequest;
import com.netflix.genie.common.messages.HiveConfigResponse;
import com.netflix.genie.common.model.HiveConfigElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.HiveConfigService;
import com.netflix.niws.client.http.HttpClientRequest.Verb;

/**
 * OpenJPA based implementation of the HiveConfigService.
 *
 * @author skrishnan
 */
public class PersistentHiveConfigImpl implements HiveConfigService {

    private static Logger logger = LoggerFactory
            .getLogger(PersistentHiveConfigImpl.class);

    private PersistenceManager<HiveConfigElement> pm;

    /**
     * Default constructor.
     */
    public PersistentHiveConfigImpl() {
        // instantiate PersistenceManager
        pm = new PersistenceManager<HiveConfigElement>();
    }

    /** {@inheritDoc} */
    @Override
    public HiveConfigResponse getHiveConfig(String id) {
        logger.info("called");
        return getHiveConfig(id, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public HiveConfigResponse getHiveConfig(String id, String name, String type) {
        logger.info("called");
        HiveConfigResponse hcr = null;

        try {
            hcr = new HiveConfigResponse();
            Object[] results;

            if ((id == null) && (name == null) && (type == null)) {
                // return all
                logger.info("GENIE: Returning all hiveConfig elements");

                // Perform a simple query for all the entities
                QueryBuilder builder = new QueryBuilder()
                        .table("HiveConfigElement");
                results = pm.query(builder);

                // set up a specific message
                hcr.setMessage("Returning all hiveConfig elements");
            } else {
                // do some filtering
                logger.info("GENIE: Returning config for {id, name, type}: "
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
                        hcr = new HiveConfigResponse(new CloudServiceException(
                                HttpURLConnection.HTTP_BAD_REQUEST,
                                "Type can only be PROD, TEST or UNITTEST"));
                        return hcr;
                    }
                    criteria.append("type = '" + type.toUpperCase() + "'");
                }

                // Get all the results as an array
                QueryBuilder builder = new QueryBuilder().table(
                        "HiveConfigElement").clause(criteria.toString());
                results = pm.query(builder);
            }

            if (results.length == 0) {
                hcr = new HiveConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No hiveConfigs found for input parameters"));
                logger.error(hcr.getErrorMsg());
                return hcr;
            } else {
                hcr.setMessage("Returning hiveConfigs for input parameters");
            }

            HiveConfigElement[] elements = new HiveConfigElement[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (HiveConfigElement) results[i];
            }
            hcr.setHiveConfigs(elements);
            return hcr;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            hcr = new HiveConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Received exception: " + e.getMessage()));
            return hcr;
        }
    }

    /** {@inheritDoc} */
    @Override
    public HiveConfigResponse createHiveConfig(HiveConfigRequest request) {
        logger.info("called");
        return createUpdateConfig(request, Verb.POST);
    }

    /** {@inheritDoc} */
    @Override
    public HiveConfigResponse updateHiveConfig(HiveConfigRequest request) {
        logger.info("called");
        return createUpdateConfig(request, Verb.PUT);
    }

    /** {@inheritDoc} */
    @Override
    public HiveConfigResponse deleteHiveConfig(String id) {
        logger.info("called");
        HiveConfigResponse hcr = null;

        if (id == null) {
            // basic error checking
            hcr = new HiveConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: id"));
            logger.error(hcr.getErrorMsg());
            return hcr;
        } else {
            // do some filtering
            logger.info("GENIE: Deleting hiveConfig for id: " + id);

            try {
                // delete the entity
                HiveConfigElement element = pm.deleteEntity(id,
                        HiveConfigElement.class);

                if (element == null) {
                    hcr = new HiveConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No hiveConfig exists for id: " + id));
                    logger.error(hcr.getErrorMsg());
                    return hcr;
                } else {
                    // all good - create a response
                    hcr = new HiveConfigResponse();
                    hcr.setMessage("Successfully deleted hiveConfig for id: "
                            + id);
                    HiveConfigElement[] elements = new HiveConfigElement[] {element};
                    hcr.setHiveConfigs(elements);
                    return hcr;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                hcr = new HiveConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getMessage()));
                return hcr;
            }
        }
    }

    /**
     * Common private method called by the create and update Can use either
     * method to create/update resource.
     */
    private HiveConfigResponse createUpdateConfig(HiveConfigRequest request,
            Verb method) {
        logger.info("called");
        HiveConfigResponse hcr = null;
        HiveConfigElement hiveConfigElement = request.getHiveConfig();
        // ensure that the element is not null
        if (hiveConfigElement == null) {
            hcr = new HiveConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing hiveConfig object"));
            logger.error(hcr.getErrorMsg());
            return hcr;
        }

        // generate/validate id for request
        String id = hiveConfigElement.getId();
        if (id == null) {
            if (method.equals(Verb.POST)) {
                id = UUID.randomUUID().toString();
                hiveConfigElement.setId(id);
            } else {
                hcr = new HiveConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "Missing required parameter for PUT: id"));
                logger.error(hcr.getErrorMsg());
                return hcr;
            }
        }

        // basic error checking
        String user = hiveConfigElement.getUser();
        if (user == null) {
            hcr = new HiveConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need required param 'user' for create/update"));
            logger.error(hcr.getErrorMsg());
            return hcr;
        }

        // ensure that the type is valid
        String type = hiveConfigElement.getType();
        if ((type != null) && (Types.Configuration.parse(type) == null)) {
            hcr = new HiveConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Invalid type - must be one of PROD, TEST or UNITTEST"));
            logger.error(hcr.getErrorMsg());
            return hcr;
        }

        // ensure that the status object is valid
        String status = hiveConfigElement.getStatus();
        if ((status != null) && (Types.ConfigStatus.parse(status) == null)) {
            hcr = new HiveConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Config status can only be ACTIVE, DEPRECATED, INACTIVE"));
            logger.error(hcr.getErrorMsg());
            return hcr;
        }

        // common error checks done - set update time before proceeding
        hiveConfigElement.setUpdateTime(System.currentTimeMillis());

        // handle POST and PUT differently
        if (method.equals(Verb.POST)) {
            try {
                initAndValidateNewElement(hiveConfigElement);
            } catch (CloudServiceException e) {
                hcr = new HiveConfigResponse(e);
                logger.error(hcr.getErrorMsg(), e);
                return hcr;

            }

            logger.info("GENIE: creating config for id: " + id);
            try {
                pm.createEntity(hiveConfigElement);

                // create a response
                hcr = new HiveConfigResponse();
                hcr.setMessage("Successfully created hiveConfig for id: " + id);
                hcr.setHiveConfigs(new HiveConfigElement[] {hiveConfigElement});
                return hcr;
            } catch (RollbackException e) {
                logger.error(e.getMessage(), e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    hcr = new HiveConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "HiveConfig already exists for id: " + id
                                    + ", use PUT to update config"));
                    return hcr;
                } else {
                    // unknown exception - send it back
                    hcr = new HiveConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "Received exception: " + e.getCause()));
                    logger.error(hcr.getErrorMsg());
                    return hcr;
                }
            }
        } else {
            // method is PUT
            logger.info("GENIE: updating config for id: " + id);

            try {
                HiveConfigElement old = pm.getEntity(id,
                        HiveConfigElement.class);
                // check if this is a create or an update
                if (old == null) {
                    try {
                        initAndValidateNewElement(hiveConfigElement);
                    } catch (CloudServiceException e) {
                        hcr = new HiveConfigResponse(e);
                        logger.error(hcr.getErrorMsg(), e);
                        return hcr;

                    }
                }
                hiveConfigElement = pm.updateEntity(hiveConfigElement);

                // all good - create a response
                hcr = new HiveConfigResponse();
                hcr.setMessage("Successfully updated hiveConfig for id: " + id);
                hcr.setHiveConfigs(new HiveConfigElement[] {hiveConfigElement});
                return hcr;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                // unknown exception - send it back
                hcr = new HiveConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getCause()));
                return hcr;
            }
        }
    }

    /**
     * Initialize and validate new element.
     *
     * @param hiveConfigElement
     *            the element to initialize
     * @throws CloudServiceException
     *             if some params are missing - else initialize, and set
     *             creation time
     */
    private void initAndValidateNewElement(HiveConfigElement hiveConfigElement)
            throws CloudServiceException {

        // basic error checking
        String name = hiveConfigElement.getName();
        String s3HiveSiteXml = hiveConfigElement.getS3HiveSiteXml();
        Types.Configuration type = Types.Configuration.parse(hiveConfigElement
                .getType());

        if ((name == null) || (s3HiveSiteXml == null) || (type == null)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name, s3HiveSiteXml, type}");
        } else {
            hiveConfigElement.setCreateTime(hiveConfigElement.getUpdateTime());
        }
    }
}
