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
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.common.model.Command;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sample client demonstrating usage of the Application Service Client.
 *
 * @author tgianos
 */
public final class ApplicationServiceSampleClient {

    /**
     * The id for the sample application.
     */
    protected static final String ID = "mr2";
    /**
     * The name for the sample application.
     */
    protected static final String APP_NAME = "MapReduce2";
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationServiceSampleClient.class);
    private static final String APP_VERSION = "1.0";

    /**
     * Private.
     */
    private ApplicationServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code.
     *
     * @param args program arguments
     * @throws Exception On issue.
     */
    public static void main(final String[] args) throws Exception {

//        //Initialize Eureka, if it is being used
//        LOG.info("Initializing Eureka");
//        ApplicationServiceClient.initEureka("test");

        LOG.info("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genie2Client.ribbon.listOfServers",
                "localhost:7001");

        LOG.info("Initializing ApplicationServiceClient");
        final ApplicationServiceClient appClient = ApplicationServiceClient.getInstance();

        LOG.info("Creating new application config");
        final Application app1 = appClient.createApplication(getSampleApplication(null));
        LOG.info("Application configuration created with id: " + app1.getId());
        LOG.info(app1.toString());

        LOG.info("Getting Applications using specified filter criteria name =  " + APP_NAME);
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", APP_NAME);
        final List<Application> appResponses = appClient.getApplications(params);
        if (appResponses.isEmpty()) {
            LOG.info("No applications found for specified criteria.");
        } else {
            LOG.info("Applications found:");
            for (final Application appResponse : appResponses) {
                LOG.info(appResponse.toString());
            }
        }

        LOG.info("Getting application config by id");
        final Application app2 = appClient.getApplication(app1.getId());
        LOG.info(app2.toString());

        LOG.info("Updating existing application config");
        app2.setStatus(ApplicationStatus.INACTIVE);
        final Application app3 = appClient.updateApplication(app1.getId(), app2);
        LOG.info(app3.toString());

        LOG.info("Configurations for application with id " + app1.getId());
        final Set<String> configs = appClient.getConfigsForApplication(app1.getId());
        for (final String config : configs) {
            LOG.info("Config = " + config);
        }

        LOG.info("Adding configurations to application with id " + app1.getId());
        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add("someNewConfigFile");
        newConfigs.add("someOtherNewConfigFile");
        final Set<String> configs2 = appClient.addConfigsToApplication(app1.getId(), newConfigs);
        for (final String config : configs2) {
            LOG.info("Config = " + config);
        }

        LOG.info("Updating set of configuration files associated with id " + app1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> configs3 = appClient.updateConfigsForApplication(app1.getId(), newConfigs);
        for (final String config : configs3) {
            LOG.info("Config = " + config);
        }

        LOG.info("Deleting all the configuration files from the application with id " + app1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> configs4 = appClient.removeAllConfigsForApplication(app1.getId());
        for (final String config : configs4) {
            //Shouldn't print anything
            LOG.info("Config = " + config);
        }

        /**************** Begin tests for tag Api's *********************/
        LOG.info("Get tags for application with id " + app1.getId());
        final Set<String> tags = app1.getTags();
        for (final String tag : tags) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Adding tags to application with id " + app1.getId());
        final Set<String> newTags = new HashSet<>();
        newTags.add("tag1");
        newTags.add("tag2");
        final Set<String> tags2 = appClient.addTagsToApplication(app1.getId(), newTags);
        for (final String tag : tags2) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Updating set of tags associated with id " + app1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> tags3 = appClient.updateTagsForApplication(app1.getId(), newTags);
        for (final String tag : tags3) {
            LOG.info("Tag = " + tag);
        }

        LOG.info("Deleting one tag from the application with id " + app1.getId());
        //This should remove the "tag3" from the tags
        final Set<String> tags5 = appClient.removeTagForApplication(app1.getId(), "tag1");
        for (final String tag : tags5) {
            //Shouldn't print anything
            LOG.info("Tag = " + tag);
        }

        LOG.info("Deleting all the tags from the application with id " + app1.getId());
        //This should remove the original config leaving only the two in this set
        final Set<String> tags4 = appClient.removeAllConfigsForApplication(app1.getId());
        for (final String tag : tags4) {
            //Shouldn't print anything
            LOG.info("Config = " + tag);
        }
        /********************** End tests for tag Api's **********************/

        LOG.info("Jars for application with id " + app1.getId());
        final Set<String> jars = appClient.getJarsForApplication(app1.getId());
        for (final String jar : jars) {
            LOG.info("jar = " + jar);
        }

        LOG.info("Adding jars to application with id " + app1.getId());
        final Set<String> newJars = new HashSet<>();
        newJars.add("someNewJarFile.jar");
        newJars.add("someOtherNewJarFile.jar");
        final Set<String> jars2 = appClient.addJarsToApplication(app1.getId(), newJars);
        for (final String jar : jars2) {
            LOG.info("jar = " + jar);
        }

        LOG.info("Updating set of jars associated with id " + app1.getId());
        //This should remove the original jar leaving only the two in this set
        final Set<String> jars3 = appClient.updateJarsForApplication(app1.getId(), newJars);
        for (final String jar : jars3) {
            LOG.info("jar = " + jar);
        }

        LOG.info("Deleting all the jars from the application with id " + app1.getId());
        //This should remove the original jar leaving only the two in this set
        final Set<String> jars4 = appClient.removeAllJarsForApplication(app1.getId());
        for (final String jar : jars4) {
            //Shouldn't print anything
            LOG.info("jar = " + jar);
        }

        LOG.info("Getting the commands associated with id " + app1.getId());
        final Set<Command> commands = appClient.getCommandsForApplication(app1.getId());
        for (final Command command : commands) {
            LOG.info("Command: " + command.toString());
        }

        LOG.info("Deleting application using id");
        final Application app4 = appClient.deleteApplication(app1.getId());
        LOG.info("Deleted application with id: " + app4.getId());
        LOG.info(app4.toString());

        LOG.info("Done");
    }

    /**
     * Helper method to quickly create an application for use in samples.
     *
     * @param id The id to use or null/empty if want one created.
     * @return A sample application with id MR2
     * @throws GenieException For any issue
     */
    public static Application getSampleApplication(final String id) throws GenieException {
        final Application app = new Application(APP_NAME, "tgianos", APP_VERSION, ApplicationStatus.ACTIVE);
        if (StringUtils.isNotBlank(id)) {
            app.setId(id);
        }
        app.setVersion("2.4.0");
        final Set<String> configs = new HashSet<>();
        configs.add("s3://mybucket/mapred-site.xml");
        app.setConfigs(configs);

        final Set<String> jars = new HashSet<>();
        jars.add("s3://mybucket/foo.jar");
        app.setJars(jars);

        final Set<String> tags = new HashSet<>();
        tags.add("tag0");
        app.setTags(tags);
        return app;
    }
}
