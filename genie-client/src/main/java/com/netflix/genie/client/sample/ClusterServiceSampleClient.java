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
package com.netflix.genie.client.sample;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.client.ApplicationServiceClient;
import com.netflix.genie.client.ClusterServiceClient;
import com.netflix.genie.client.CommandServiceClient;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Types.ClusterStatus;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sample client demonstrating usage of the Cluster Configuration Service
 * Client.
 *
 * @author skrishnan
 * @author tgianos
 */
public final class ClusterServiceSampleClient {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterServiceSampleClient.class);
    protected static final String ID = "bdp_hquery_20140505_185527";
    protected static final String NAME = "h2query";

    private ClusterServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code.
     *
     * @param args
     * @throws java.lang.Exception
     */
    public static void main(final String[] args) throws Exception {

        // Initialize Eureka, if it is being used
        // LOG.info("Initializing Eureka");
        // ClusterServiceClient.initEureka("test");
        LOG.info("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genieClient.ribbon.listOfServers",
                "localhost:7001");

        LOG.info("Initializing ApplicationServiceClient");
        final ApplicationServiceClient appClient = ApplicationServiceClient.getInstance();

        final Application app1 = appClient.createApplication(
                ApplicationServiceSampleClient.getSampleApplication(
                        ApplicationServiceSampleClient.ID
                ));
        LOG.info("Created application:");
        LOG.info(app1.toString());

        final Application app2 = appClient.createApplication(
                ApplicationServiceSampleClient.getSampleApplication(
                        ApplicationServiceSampleClient.ID + "2"
                ));
        LOG.info("Created application:");
        LOG.info(app2.toString());
        LOG.info("Initializing CommandServiceClient");
        final CommandServiceClient commandClient = CommandServiceClient.getInstance();

        LOG.info("Creating command pig13_mr2");
        final Set<Application> apps = new HashSet<Application>();
        apps.add(app1);
        apps.add(app2);
        final Command command1 = commandClient.createCommand(
                CommandServiceSampleClient.createSampleCommand(
                        CommandServiceSampleClient.ID, apps));
        LOG.info("Created command:");
        LOG.info(command1.toString());
        final Set<Command> commands = new HashSet<Command>();
        commands.add(command1);

        LOG.info("Initializing ClusterConfigServiceClient");
        final ClusterServiceClient clusterClient = ClusterServiceClient.getInstance();

        LOG.info("Creating new cluster configuration");
        final Cluster cluster1 = clusterClient.createCluster(createSampleCluster(ID, commands));
        LOG.info("Cluster config created with id: " + cluster1.getId());
        LOG.info(cluster1.toString());

        LOG.info("Getting cluster config by id");
        final Cluster cluster2 = clusterClient.getCluster(cluster1.getId());
        LOG.info(cluster2.toString());

        LOG.info("Getting clusterConfigs using specified filter criteria");
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", NAME);
        params.put("adHoc", "false");
        params.put("test", "true");
        params.put("limit", "3");
        final List<Cluster> clusters = clusterClient.getClusterConfigs(params);
        if (clusters != null && !clusters.isEmpty()) {
            for (final Cluster cluster : clusters) {
                LOG.info(cluster.toString());
            }
        } else {
            LOG.info("No clusters found for parameters");
        }

        LOG.info("Updating existing cluster config");
        cluster2.setStatus(ClusterStatus.TERMINATED);
        final Cluster cluster3 = clusterClient.updateCluster(cluster2.getId(), cluster2);
        LOG.info("Cluster updated:");
        LOG.info(cluster3.toString());

        LOG.info("Deleting cluster config using id");
        final Cluster cluster4 = clusterClient.deleteClusterConfig(cluster1.getId());
        LOG.info("Deleted cluster config with id: " + cluster1.getId());
        LOG.info(cluster4.toString());

        LOG.info("Deleting command config using id");
        final Command command5 = commandClient.deleteCommand(command1.getId());
        LOG.info("Deleted command config with id: " + command1.getId());
        LOG.info(command5.toString());

        LOG.info("Deleting application config using id");
        final Application app3 = appClient.deleteApplication(app1.getId());
        LOG.info("Deleted application config with id: " + app1.getId());
        LOG.info(app3.toString());

        LOG.info("Deleting application config using id");
        final Application app4 = appClient.deleteApplication(app2.getId());
        LOG.info("Deleted application config with id: " + app2.getId());
        LOG.info(app4.toString());

        LOG.info("Done");
    }

    public static Cluster createSampleCluster(final String id, final Set<Command> commands) throws CloudServiceException {
        final Set<String> configs = new HashSet<String>();
        configs.add("s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/core-site.xml");
        configs.add("s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/hdfs-site.xml");
        configs.add("s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/yarn-site.xml");
        final Cluster cluster = new Cluster(
                NAME,
                "tgianos",
                ClusterStatus.OUT_OF_SERVICE,
                "com.netflix.genie.server.jobmanager.impl.YarnJobManager",
                configs);
        if (StringUtils.isNotEmpty(id)) {
            cluster.setId(id);
        }
        final Set<String> tags = new HashSet<String>();
        tags.add("adhoc");
        tags.add("h2query");
        tags.add(cluster.getId());
        cluster.setTags(tags);
        cluster.setVersion("2.4.0");
        if (commands != null && !commands.isEmpty()) {
            cluster.setCommands(commands);
        }
        return cluster;
    }
}
