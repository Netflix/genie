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
import com.netflix.genie.client.CommandServiceClient;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sample client demonstrating usage of the Command Service Client.
 *
 * @author tgianos
 * @author amsharma
 */
public final class CommandServiceSampleClient {

    /**
     * Unique id for the sample command.
     */
    protected static final String ID = "pig13_mr2";
    /**
     * Name for the sample command.
     */
    protected static final String CMD_NAME = "pig";
    private static final Logger LOG = LoggerFactory.getLogger(CommandServiceSampleClient.class);
    private static final String CMD_VERSION = "1.0";

    /**
     * Private constructor.
     */
    private CommandServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code.
     *
     * @param args program arguments
     * @throws Exception on issue.
     */
    public static void main(final String[] args) throws Exception {

        // Initialize Eureka, if it is being used
        // LOG.info("Initializing Eureka");
        // ApplicationServiceClient.initEureka("test");
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
        final Command command1 = commandClient.createCommand(createSampleCommand(ID));
        commandClient.setApplicationForCommand(command1.getId(), app1);
        LOG.info("Created command:");
        LOG.info(command1.toString());

        LOG.info("Getting Commands using specified filter criteria name =  " + CMD_NAME);
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", CMD_NAME);
        final List<Command> commands = commandClient.getCommands(params);
        if (commands.isEmpty()) {
            LOG.info("No commands found for specified criteria.");
        } else {
            LOG.info("Commands found:");
            for (final Command command : commands) {
                LOG.info(command.toString());
            }
        }

        LOG.info("Getting command config by id");
        final Command command3 = commandClient.getCommand(ID);
        LOG.info(command3.toString());

        LOG.info("Updating existing command config");
        command3.setStatus(CommandStatus.INACTIVE);
        final Command command4 = commandClient.updateCommand(ID, command3);
        LOG.info(command4.toString());

        LOG.info("Configurations for command with id " + command1.getId());
        final Set<String> configs = commandClient.getConfigsForCommand(command1.getId());
        for (final String config : configs) {
            LOG.info("Config = " + config);
        }

        LOG.info("Adding configurations to command with id " + command1.getId());
        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add("someNewConfigFile");
        newConfigs.add("someOtherNewConfigFile");
        final Set<String> configs2 = commandClient.addConfigsToCommand(command1.getId(), newConfigs);
        for (final String config : configs2) {
            LOG.info("Config = " + config);
        }

        LOG.info("Updating set of configuration files associated with id " + command1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> configs3 = commandClient.updateConfigsForCommand(command1.getId(), newConfigs);
        for (final String config : configs3) {
            LOG.info("Config = " + config);
        }

        LOG.info("Deleting all the configuration files from the command with id " + command1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> configs4 = commandClient.removeAllConfigsForCommand(command1.getId());
        for (final String config : configs4) {
            //Shouldn't print anything
            LOG.info("Config = " + config);
        }

        /**************** Begin tests for tag Api's *********************/
        LOG.info("Get tags for command with id " + command1.getId());
        final Set<String> tags = command1.getTags();
        for (final String tag : tags) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Adding tags to command with id " + command1.getId());
        final Set<String> newTags = new HashSet<>();
        newTags.add("tag1");
        newTags.add("tag2");
        final Set<String> tags2 = commandClient.addTagsToCommand(command1.getId(), newTags);
        for (final String tag : tags2) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Updating set of tags associated with id " + command1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> tags3 = commandClient.updateTagsForCommand(command1.getId(), newTags);
        for (final String tag : tags3) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Deleting one tag from the command with id " + command1.getId());
        //This should remove the "tag3" from the tags
        final Set<String> tags5 = commandClient.removeTagForCommand(command1.getId(), "tag1");
        for (final String tag : tags5) {
            //Shouldn't print anything
            LOG.info("Tag = " + tag);
        }

        LOG.info("Deleting all the tags from the command with id " + command1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> tags4 = commandClient.removeAllConfigsForCommand(command1.getId());
        for (final String tag : tags4) {
            //Shouldn't print anything
            LOG.info("Config = " + tag);
        }
        /********************** End tests for tag Api's **********************/

        LOG.info("Application for command with id " + command1.getId());
        final Application application = commandClient.getApplicationForCommand(command1.getId());
        LOG.info("Application = " + application);

        LOG.info("Removing Application for command with id " + command1.getId());
        final Application application2 = commandClient.removeApplicationForCommand(command1.getId());
        LOG.info("Application = " + application2);

        LOG.info("Getting all the clusters for command with id  " + command1.getId());
        final Set<Cluster> clusters = commandClient.getClustersForCommand(command1.getId());
        for (final Cluster cluster : clusters) {
            LOG.info("Cluster = " + cluster);
        }

        LOG.info("Deleting command config using id");
        final Command command5 = commandClient.deleteCommand(ID);
        LOG.info("Deleted command config with id: " + ID);
        LOG.info(command5.toString());

        LOG.info("Deleting all applications");
        final List<Application> deletedApps = appClient.deleteAllApplications();
        LOG.info("Deleted all applications");
        LOG.info(deletedApps.toString());

        LOG.info("Done");
    }

    /**
     * Create a sample command and attach the the supplied applications.
     *
     * @param id The id to use or null if want one created.
     * @return The pig example command
     * @throws GenieException On configuration issue.
     */
    public static Command createSampleCommand(
            final String id) throws GenieException {
        final Command command = new Command(
                CMD_NAME,
                "tgianos",
                CMD_VERSION,
                CommandStatus.ACTIVE,
                "/apps/pig/0.13/bin/pig"
        );
        if (StringUtils.isNotBlank(id)) {
            command.setId(id);
        }
        command.setEnvPropFile("s3:/mybucket/envFile.sh");
        command.setVersion("0.13");

        final Set<String> tags = new HashSet<>();
        tags.add("tag0");
        command.setTags(tags);
        return command;
    }
}
