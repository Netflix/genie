package com.netflix.genie.server.services.impl;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.persistence.EntityExistsException;
import javax.persistence.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ClusterConfigElement;
import com.netflix.genie.common.model.CommandConfigElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.common.model.Types.ClusterStatus;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ConfigServiceFactory;

/**
 * @author skrishnan
 * @author amsharma
 */
public class PersistentClusterConfigImpl implements ClusterConfigService {
    
    private static Logger logger = LoggerFactory
            .getLogger(PersistentClusterConfigImpl.class);

    private PersistenceManager<ClusterConfigElement> pm;

    /**
     * Default constructor - initialize all required dependencies.
     *
     * @throws CloudServiceException
     */
    public PersistentClusterConfigImpl() throws CloudServiceException {
        // instantiate PersistenceManager
        pm = new PersistenceManager<ClusterConfigElement>();
    }
    
    @Override
    public ClusterConfigResponse getClusterConfig(String id) {
        logger.info("called");

        ClusterConfigResponse ccr;
        ClusterConfigElement cce;
        try {
            cce = pm.getEntity(id, ClusterConfigElement.class);
        } catch (Exception e) {
            logger.error("Failed to get cluster config: ", e);
            ccr = new ClusterConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return ccr;
        }

        if (cce == null) {
            String msg = "Cluster config not found: " + id;
            logger.error(msg);
            ccr = new ClusterConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return ccr;
        } else {
            ccr = new ClusterConfigResponse();
            ccr.setClusterConfigs(new ClusterConfigElement[] {cce});
            ccr.setMessage("Returning cluster config for: " + id);
            return ccr;
        }
    }

    @Override
    public ClusterConfigResponse getClusterConfig(String id, String name,
            String commandId, List<String> tags, ClusterStatus status) {
        
        List<String> statusList = Arrays.asList(status.name());
        return getClusterConfig(id, name, statusList, tags,
                null, null, null, null);
    }

    @Override
    public ClusterConfigResponse getClusterConfig(String id, String name,
            List<String> status, List<String> tags, Long minUpdateTime,
            Long maxUpdateTime, Integer limit, Integer page) {
        
        logger.info("called");

        ClusterConfigResponse ccr = null;
        try {
            logger.info("GENIE: Returning configs for specified params");

            ccr = new ClusterConfigResponse();
            Object[] results;

            // construct query
            ClauseBuilder criteria = new ClauseBuilder(ClauseBuilder.AND);
            if ((id != null) && (!id.isEmpty())) {
                criteria.append("id like '" + id + "'");
            }
            if ((name != null) && (!name.isEmpty())) {
                criteria.append("name like '" + name + "'");
            }
            if (minUpdateTime != null) {
                criteria.append("updateTime >= " + minUpdateTime);
            }
            if (maxUpdateTime != null) {
                criteria.append("updateTime <= " + maxUpdateTime);
            }
            
            if ((status != null) && !status.isEmpty()) {
                int count = 0;
                ClauseBuilder statusCriteria = new ClauseBuilder(ClauseBuilder.OR);
                Iterator<String> it = status.iterator();
                while (it.hasNext()) {
                    String next = it.next();
                    if ((next == null) || (next.isEmpty())) {
                        continue;
                    }
                    if (Types.ClusterStatus.parse(next) == null) {
                        ccr = new ClusterConfigResponse(
                                new CloudServiceException(
                                        HttpURLConnection.HTTP_BAD_REQUEST,
                                        "Cluster status: " + next
                                        + " can only be UP, OUT_OF_SERVICE or TERMINATED"));
                        logger.error(ccr.getErrorMsg());
                        return ccr;
                    }
                    statusCriteria.append("status = '" + next.toUpperCase() + "'");
                    count++;
                }
                if (count > 0) {
                    criteria.append("(" + statusCriteria.toString() + ")", false);
                }
            }
            
            // Get all the results as an array
            String criteriaString = criteria.toString();
            logger.info("Criteria: " + criteriaString);
            QueryBuilder builder = new QueryBuilder()
                    .table("ClusterConfigElement").clause(criteriaString)
                    .limit(limit).page(page);
            results = pm.query(builder);

            if (results.length == 0) {
                ccr = new ClusterConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No clusterConfigs found for input parameters"));
                logger.error(ccr.getErrorMsg());
                return ccr;
            } else {
                ccr.setMessage("Returning clusterConfigs for input parameters");
            }

            ClusterConfigElement[] elements = new ClusterConfigElement[results.length];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = (ClusterConfigElement) results[i];
            }
            ccr.setClusterConfigs(elements);
            return ccr;
            
        }catch (Exception e) {
            logger.error(e.getMessage(), e);
            ccr = new ClusterConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Received exception: " + e.getMessage()));
            return ccr;
        }      
    }

    @Override
    public ClusterConfigResponse createClusterConfig(
            ClusterConfigRequest request) {
        logger.info("called");
        return createUpdateConfig(request, Verb.POST);
    }

    @Override
    public ClusterConfigResponse updateClusterConfig(
            ClusterConfigRequest request) {
        logger.info("called");
        return createUpdateConfig(request, Verb.PUT);
    }

    @Override
    public ClusterConfigResponse deleteClusterConfig(String id) {
        logger.info("called");
        ClusterConfigResponse ccr = null;

        if (id == null) {
            // basic error checking
            ccr = new ClusterConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: id"));
            logger.error(ccr.getErrorMsg());
        } else {
            // do some filtering
            logger.info("GENIE: Deleting clusterConfig for id: " + id);
            try {
                ClusterConfigElement element = pm.deleteEntity(id,
                        ClusterConfigElement.class);
                if (element == null) {
                    // element doesn't exist
                    ccr = new ClusterConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_NOT_FOUND,
                            "No clusterConfig exists for id: " + id));
                    logger.error(ccr.getErrorMsg());
                } else {
                    // all good - create a response
                    ccr = new ClusterConfigResponse();
                    ccr.setMessage("Successfully deleted clusterConfig for id: "
                            + id);
                    ClusterConfigElement[] elements = new ClusterConfigElement[] {element};
                    ccr.setClusterConfigs(elements);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                // send the exception back
                ccr = new ClusterConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getMessage()));
            }
        }
        return ccr;
    }

    /*
     * Common private method called by the create and update Can use either
     * method to create/update resource.
     */
    private ClusterConfigResponse createUpdateConfig(
            ClusterConfigRequest request, Verb method) {
        logger.debug("called");
        ClusterConfigResponse ccr = null;
        ClusterConfigElement clusterConfigElement = request.getClusterConfig();
        // ensure that the element is not null
        if (clusterConfigElement == null) {
            ccr = new ClusterConfigResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing clusterConfig object"));
            logger.error(ccr.getErrorMsg());
            return ccr;
        }

        // generate/validate id for request
        String id = clusterConfigElement.getId();
        if (id == null) {
            if (method.equals(Verb.POST)) {
                // create UUID for POST, if it doesn't exist
                id = UUID.randomUUID().toString();
                clusterConfigElement.setId(id);
            } else {
                ccr = new ClusterConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST,
                        "Missing required parameter for PUT: id"));
                logger.error(ccr.getErrorMsg());
                return ccr;
            }
        }

        // more error checking
        if (clusterConfigElement.getUser() == null) {
            ccr = new ClusterConfigResponse(
                    new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST,
                            "Missing parameter 'user' for creating/updating clusterConfig"));
            logger.error(ccr.getErrorMsg());
            return ccr;
        }

        // ensure that the status object is valid
        String status = clusterConfigElement.getStatus();
        if ((status != null) && (Types.ClusterStatus.parse(status) == null)) {
            ccr = new ClusterConfigResponse(
                    new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST,
                            "Cluster status can only be UP, OUT_OF_SERVICE or TERMINATED"));
            logger.error(ccr.getErrorMsg());
            return ccr;
        }

        // ensure that child command configs exist
        try {
            validateChildren(clusterConfigElement);
        } catch (CloudServiceException cse) {
            ccr = new ClusterConfigResponse(cse);
            logger.error(ccr.getErrorMsg(), cse);
            return ccr;
        }

        // common error checks done - set update time before proceeding
        clusterConfigElement.setUpdateTime(System.currentTimeMillis());

        // handle POST and PUT differently
        if (method.equals(Verb.POST)) {
            logger.info("GENIE: creating config for id: " + id);

            // validate/initialize new element
            try {
                initAndValidateNewElement(clusterConfigElement);
            } catch (CloudServiceException e) {
                ccr = new ClusterConfigResponse(e);
                logger.error(ccr.getErrorMsg(), e);
                return ccr;
            }

            // now create the new element
            try {
                pm.createEntity(clusterConfigElement);

                // create a response
                ccr = new ClusterConfigResponse();
                ccr.setMessage("Successfully created clusterConfig for id: "
                        + id);
                ccr.setClusterConfigs(new ClusterConfigElement[] {clusterConfigElement});
                return ccr;
            } catch (RollbackException e) {
                logger.error(e.getMessage(), e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    ccr = new ClusterConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "ClusterConfig already exists for id: " + id
                                    + ", use PUT to update config"));
                    return ccr;
                } else {
                    // unknown exception - send it back
                    ccr = new ClusterConfigResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "Received exception: " + e.getCause()));
                    logger.error(ccr.getErrorMsg());
                    return ccr;
                }
            }
        } else {
            // method is PUT
            logger.info("GENIE: updating config for id: " + id);

            try {
                ClusterConfigElement old = pm.getEntity(id,
                        ClusterConfigElement.class);
                // check if this is a create or an update
                if (old == null) {
                    try {
                        initAndValidateNewElement(clusterConfigElement);
                    } catch (CloudServiceException e) {
                        ccr = new ClusterConfigResponse(e);
                        logger.error(ccr.getErrorMsg(), e);
                        return ccr;
                    }
                }
                clusterConfigElement = pm.updateEntity(clusterConfigElement);

                // all good - create a response
                ccr = new ClusterConfigResponse();
                ccr.setMessage("Successfully updated clusterConfig for id: "
                        + id);
                ccr.setClusterConfigs(new ClusterConfigElement[] {clusterConfigElement});
                return ccr;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                // send the exception back
                ccr = new ClusterConfigResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR,
                        "Received exception: " + e.getCause()));
                return ccr;
            }
        }
    }
    
    private void validateChildren(ClusterConfigElement clusterConfigElement) 
            throws CloudServiceException {

        ArrayList<String> cmdIds = clusterConfigElement.getCmdIds();

        if(cmdIds != null) {
            PersistenceManager<CommandConfigElement> pma = new PersistenceManager<CommandConfigElement>();
            ArrayList<CommandConfigElement> cmdList = new ArrayList<CommandConfigElement>();
            Iterator<String> it = cmdIds.iterator();
            while(it.hasNext()) {
                String cmdId = (String)it.next();
                CommandConfigElement cmde = (CommandConfigElement)pma.getEntity(cmdId, CommandConfigElement.class);
                if (cmde != null) {
                    cmdList.add(cmde);
                } else {
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST,
                            "Command Does Not Exist: {" + cmdId +"}");
                }               
            }
            clusterConfigElement.setCommands(cmdList);
        } else {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No commandId's specified for the cluster");
        }
    }

    /*
     * Throws exception if all required params are not present - else
     * initialize, and set creation time.
     */
    private void initAndValidateNewElement(
            ClusterConfigElement clusterConfigElement)
            throws CloudServiceException {
        
        //TODO Figure out all mandatory parameters for a cluster
        if ((clusterConfigElement.getName() == null)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required Hadoop parameters for creating clusterConfig: "
                            + "{name, s3MapredSiteXml, s3HdfsSiteXml, s3CoreSiteXml}");
        } else {
            clusterConfigElement.setCreateTime(clusterConfigElement
                    .getUpdateTime());

            // if booleans are received as NULL, set them to false, which is the
            // default
//            if (clusterConfigElement.getAdHoc() == null) {
//                clusterConfigElement.setAdHoc(Boolean.FALSE);
//            }
//            if (clusterConfigElement.getSla() == null) {
//                clusterConfigElement.setSla(Boolean.FALSE);
//            }
//            if (clusterConfigElement.getBonus() == null) {
//                clusterConfigElement.setBonus(Boolean.FALSE);
//            }
//            if (clusterConfigElement.getProd() == null) {
//                clusterConfigElement.setProd(Boolean.FALSE);
//            }
//            if (clusterConfigElement.getTest() == null) {
//                clusterConfigElement.setTest(Boolean.FALSE);
//            }
//            if (clusterConfigElement.getUnitTest() == null) {
//                clusterConfigElement.setUnitTest(Boolean.FALSE);
//            }
        }
    }
}
