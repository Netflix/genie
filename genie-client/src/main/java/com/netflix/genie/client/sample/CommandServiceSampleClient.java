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
import com.netflix.genie.client.CommandServiceClient;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Types.CommandStatus;

import java.util.ArrayList;
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

    private static final Logger LOG = LoggerFactory.getLogger(CommandServiceSampleClient.class);

    /**
     * Unique id for the sample command.
     */
    protected static final String ID = "pig13_mr2";

    /**
     * Name for the sample command.
     */
    protected static final String CMD_NAME = "pig";

    /**
     * Private constructor.
     */
    private CommandServiceSampleClient() {
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
        // ApplicationServiceClient.initEureka("test");
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
        final List<Application> apps = new ArrayList<Application>();
        apps.add(app1);
        apps.add(app2);
        final Command command1 = commandClient.createCommand(createSampleCommand(ID, apps));
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
        final Set<String> newConfigs = new HashSet<String>();
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

        LOG.info("Applications for command with id " + command1.getId());
        final Set<Application> applications = commandClient.getApplicationsForCommand(command1.getId());
        for (final Application application : applications) {
            LOG.info("Application = " + application);
        }

        LOG.info("Adding applications to command with id " + command1.getId());
        final Set<Application> newApps = new HashSet<Application>();
        newApps.add(ApplicationServiceSampleClient.getSampleApplication(ID + "something"));
        newApps.add(ApplicationServiceSampleClient.getSampleApplication(null));
        final Set<Application> applications2 = commandClient.addApplicationsToCommand(command1.getId(), newApps);
        for (final Application application : applications2) {
            LOG.info("Application = " + application);
        }

        LOG.info("Updating set of applications files associated with id " + command1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<Application> applications3 = commandClient.updateApplicationsForCommand(command1.getId(), newApps);
        for (final Application application : applications3) {
            LOG.info("Application = " + application);
        }

        LOG.info("Deleting the application from the command with id " + ID + "something");
        final Set<Application> applications4 =
                commandClient.removeApplicationForCommand(command1.getId(), ID + "something");
        for (final Application application : applications4) {
            LOG.info("Application = " + application);
        }

        LOG.info("Deleting all the applications from the command with id " + command1.getId());
        final Set<Application> applications5 = commandClient.removeAllApplicationsForCommand(command1.getId());
        for (final Application application : applications5) {
            //Shouldn't print anything
            LOG.info("Application = " + application);
        }

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
     * @param apps The apps to add to this command or null/empty for none.
     * @return The pig example command
     * @throws CloudServiceException
     */
    public static Command createSampleCommand(
            final String id,
            final List<Application> apps) throws CloudServiceException {
        final Command command = new Command(
                CMD_NAME,
                "tgianos",
                CommandStatus.ACTIVE,
                "/apps/pig/0.13/bin/pig");
        if (!StringUtils.isEmpty(id)) {
            command.setId(id);
        }
        command.setEnvPropFile("s3://netflix-dataoven-test/genie2/command/pig13_mr2/envFile.sh");
        command.setVersion("0.13");
        if (apps != null && !apps.isEmpty()) {
            command.setApplications(apps);
        }
        return command;
    }

    /**
     * Create a sample command.
     *
     * @param id The id to use or null if want one created.
     * @return An example command
     * @throws CloudServiceException
     */
    public static Command getSampleCommand(
            final String id)
                    throws CloudServiceException {
        final Command cmd = new Command(CMD_NAME, "amsharma", CommandStatus.ACTIVE, "/foo/exec.sh");
        if (StringUtils.isNotEmpty(id)) {
            cmd.setId(id);
        }
        cmd.setVersion("2.4.0");
        final Set<String> configs = new HashSet<String>();
        configs.add("s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/mapred-site.xml");
        cmd.setConfigs(configs);
        return cmd;
    }
}
