/*
 *
 *  Copyright 2015 Netflix, Inc.
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
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterStatus;
import com.netflix.genie.common.model.Command;

import java.util.ArrayList;
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
 * @author amsharma
 */
public final class ClusterServiceSampleClient {

    /**
     * ID for the sample cluster.
     */
    protected static final String ID = "bdp_hquery_20140505_185527";
    /**
     * Name for the sample cluster.
     */
    protected static final String NAME = "h2query";
    private static final Logger LOG = LoggerFactory.getLogger(ClusterServiceSampleClient.class);

    private ClusterServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code.
     *
     * @param args program arguments
     * @throws Exception On issue.
     */
    public static void main(final String[] args) throws Exception {
        // Initialize Eureka, if it is being used
        // LOG.info("Initializing Eureka");
        // ClusterServiceClient.initEureka("test");
        LOG.info("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genie2Client.ribbon.listOfServers",
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
        final Command command1 = commandClient.createCommand(
                CommandServiceSampleClient.createSampleCommand(
                        CommandServiceSampleClient.ID));

        commandClient.setApplicationForCommand(command1.getId(), app1);

        LOG.info("Created command:");
        LOG.info(command1.toString());
        final List<Command> commands = new ArrayList<>();
        commands.add(command1);

        LOG.info("Initializing ClusterConfigServiceClient");
        final ClusterServiceClient clusterClient = ClusterServiceClient.getInstance();

        LOG.info("Creating new cluster configuration");
        final Cluster cluster1 = clusterClient.createCluster(createSampleCluster(ID));
        clusterClient.addCommandsToCluster(cluster1.getId(), commands);

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
        final List<Cluster> clusters = clusterClient.getClusters(params);
        if (clusters != null && !clusters.isEmpty()) {
            for (final Cluster cluster : clusters) {
                LOG.info(cluster.toString());
            }
        } else {
            LOG.info("No clusters found for parameters");
        }

        LOG.info("Configurations for cluster with id " + cluster1.getId());
        final Set<String> configs = clusterClient.getConfigsForCluster(cluster1.getId());
        for (final String config : configs) {
            LOG.info("Config = " + config);
        }

        LOG.info("Adding configurations to cluster with id " + cluster1.getId());
        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add("someNewConfigFile");
        newConfigs.add("someOtherNewConfigFile");
        final Set<String> configs2 = clusterClient.addConfigsToCluster(cluster1.getId(), newConfigs);
        for (final String config : configs2) {
            LOG.info("Config = " + config);
        }

        LOG.info("Updating set of configuration files associated with id " + cluster1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> configs3 = clusterClient.updateConfigsForCluster(cluster1.getId(), newConfigs);
        for (final String config : configs3) {
            LOG.info("Config = " + config);
        }

        /**************** Begin tests for tag Api's *********************/
        LOG.info("Get tags for cluster with id " + cluster1.getId());
        final Set<String> tags = cluster1.getTags();
        for (final String tag : tags) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Adding tags to cluster with id " + cluster1.getId());
        final Set<String> newTags = new HashSet<>();
        newTags.add("tag1");
        newTags.add("tag2");
        final Set<String> tags2 = clusterClient.addTagsToCluster(cluster1.getId(), newTags);
        for (final String tag : tags2) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Updating set of tags associated with id " + cluster1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> tags3 = clusterClient.updateTagsForCluster(cluster1.getId(), newTags);
        for (final String tag : tags3) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Deleting one tag from the cluster with id " + cluster1.getId());
        //This should remove the "tag3" from the tags
        final Set<String> tags5 = clusterClient.removeTagForCluster(cluster1.getId(), "tag1");
        for (final String tag : tags5) {
            //Shouldn't print anything
            LOG.info("Tag = " + tag);
        }

        LOG.info("Deleting all the tags from the cluster with id " + cluster1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> tags4 = clusterClient.removeAllTagsForCluster(cluster1.getId());
        for (final String tag : tags4) {
            //Shouldn't print anything
            LOG.info("Config = " + tag);
        }
        /********************** End tests for tag Api's **********************/

        LOG.info("Commands for cluster with id " + cluster1.getId());
        final List<Command> commands1 = clusterClient.getCommandsForCluster(cluster1.getId());
        for (final Command command : commands1) {
            LOG.info("Command = " + command);
        }

        LOG.info("Adding commands to cluster with id " + cluster1.getId());
        final List<Command> newCmds = new ArrayList<>();
        newCmds.add(commandClient.createCommand(CommandServiceSampleClient.createSampleCommand(ID + "something")));
        newCmds.add(commandClient.createCommand(CommandServiceSampleClient.createSampleCommand(null)));

        final List<Command> commands2 = clusterClient.addCommandsToCluster(cluster1.getId(), newCmds);
        for (final Command command : commands2) {
            LOG.info("Command = " + command);
        }

        LOG.info("Updating set of commands files associated with id " + cluster1.getId());
        //This should remove the original config leaving only the two in this set
        final List<Command> commands3 = clusterClient.updateCommandsForCluster(cluster1.getId(), newCmds);
        for (final Command command : commands3) {
            LOG.info("Command = " + command);
        }

        LOG.info("Deleting the command from the cluster with id " + ID + "something");
        final Set<Command> commands4 = clusterClient.removeCommandForCluster(cluster1.getId(), ID + "something");
        for (final Command command : commands4) {
            LOG.info("Command = " + command);
        }

        LOG.info("Deleting all the commands from the command with id " + command1.getId());
        final List<Command> commands5 = clusterClient.removeAllCommandsForCluster(cluster1.getId());
        for (final Command command : commands5) {
            //Shouldn't print anything
            LOG.info("Command = " + command);
        }

        LOG.info("Updating existing cluster config");
        cluster2.setStatus(ClusterStatus.TERMINATED);
        final Cluster cluster3 = clusterClient.updateCluster(cluster2.getId(), cluster2);
        LOG.info("Cluster updated:");
        LOG.info(cluster3.toString());

        LOG.info("Deleting cluster config using id");
        final Cluster cluster4 = clusterClient.deleteCluster(cluster1.getId());
        LOG.info("Deleted cluster config with id: " + cluster1.getId());
        LOG.info(cluster4.toString());

        LOG.info("Deleting command config using id");
        final Command command5 = commandClient.deleteCommand(command1.getId());
        LOG.info("Deleted command config with id: " + command1.getId());
        LOG.info(command5.toString());

        LOG.info("Deleting commands in newCmd");
        for (final Command cmd : newCmds) {
            commandClient.deleteCommand(cmd.getId());
        }

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

    /**
     * Create a cluster from the input parameters.
     *
     * @param id The ID to use. If null or empty one will be created.
     * @return A cluster object
     * @throws GenieException For any configuration exception.
     */
    public static Cluster createSampleCluster(
            final String id) throws GenieException {
        final Set<String> configs = new HashSet<>();
        configs.add("s3://mybucket/core-site.xml");
        configs.add("s3://mybucket/hdfs-site.xml");
        configs.add("s3://mybucketyarn-site.xml");
        final Cluster cluster = new Cluster(
                NAME,
                "tgianos",
                "2.4.0",
                ClusterStatus.OUT_OF_SERVICE,
                "com.netflix.genie.server.jobmanager.impl.YarnJobManager"
        );
        cluster.setConfigs(configs);
        if (StringUtils.isNotBlank(id)) {
            cluster.setId(id);
        }
        final Set<String> tags = new HashSet<>();
        tags.add("adhoc");
        tags.add("h2query");
        tags.add(cluster.getId());
        cluster.setTags(tags);

        return cluster;
    }
}
