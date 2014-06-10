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
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.Types.CommandStatus;
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
 */
public final class CommandServiceSampleClient {

    private static final Logger LOG = LoggerFactory.getLogger(CommandServiceSampleClient.class);
    protected static final String ID = "pig13_mr2";
    protected static final String NAME = "pig";

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
        final Set<Application> apps = new HashSet<Application>();
        apps.add(app1);
        apps.add(app2);
        final Command command1 = commandClient.createCommand(createSampleCommand(ID, apps));
        LOG.info("Created command:");
        LOG.info(command1.toString());

        LOG.info("Getting Commands using specified filter criteria name =  " + NAME);
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", NAME);
        final List<Command> commandResponses = commandClient.getCommands(params);
        if (commandResponses.isEmpty()) {
            LOG.info("No commands found for specified criteria.");
        } else {
            LOG.info("Commands found:");
            for (final Command commandResponse : commandResponses) {
                LOG.info(commandResponse.toString());
            }
        }

        LOG.info("Getting command config by id");
        final Command command3 = commandClient.getCommand(ID);
        LOG.info(command3.toString());

        LOG.info("Updating existing command config");
        command3.setStatus(CommandStatus.INACTIVE);
        final Command command4 = commandClient.updateCommand(ID, command3);
        LOG.info(command4.toString());

        LOG.info("Deleting command config using id");
        final Command command5 = commandClient.deleteCommand(ID);
        LOG.info("Deleted command config with id: " + ID);
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

    /**
     * Create a sample command and attach the the supplied applications.
     *
     * @param id The id to use or null if want one created.ß
     * @param apps The apps to add to this command or null/empty for none.
     * @return The pig example command
     * @throws CloudServiceException
     */
    public static Command createSampleCommand(final String id, final Set<Application> apps) throws CloudServiceException {
        final Command command = new Command();
        if (!StringUtils.isEmpty(id)) {
            command.setId(id);
        }
        command.setName(NAME);
        command.setStatus(CommandStatus.ACTIVE);
        command.setExecutable("/apps/pig/0.13/bin/pig");
        command.setEnvPropFile("s3://netflix-dataoven-test/genie2/command/pig13_mr2/envFile.sh");
        command.setUser("tgianos");
        command.setVersion("0.13");
        if (apps != null && !apps.isEmpty()) {
            command.setApplications(apps);
        }
        return command;
    }
}
