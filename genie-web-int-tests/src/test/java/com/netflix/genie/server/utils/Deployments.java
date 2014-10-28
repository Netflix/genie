/*
 * Copyright 2014 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package com.netflix.genie.server.utils;

import java.io.File;
import java.io.FileFilter;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.importer.ZipImporter;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Assert;

/**
 * An example test case that demonstrates injection of a simple component into a
 * test case.
 *
 * @author Jakub Narloch (jmnarloch@gmail.com)
 */
public final class Deployments {

    /**
     * Creates new instance of {@link Deployments} class.
     */
    private Deployments() {
        // empty constructor
    }

    /**
     * Creates the test deployment.
     *
     * @return the test deployment
     */
    @SuppressWarnings("rawtypes")
    public static Archive createDeployment() {

        WebArchive archive = ShrinkWrap.create(ZipImporter.class, "genie-web.war")
                .importFrom(resolveTestWar("../genie-web/"))
                .as(WebArchive.class);

        // overwrites the web.xml file
        archive.setWebXML("web.xml");
        // adds the archaius configuration
        archive.addAsResource("config.properties");
        archive.addAsResource("persistence.xml");
        archive.addAsResource("genie-application-int.xml");
        archive.addAsResource("genie-jpa-int.xml");

        return archive;
    }

    /**
     * Resolves the path to the genie-web war, that need to be build prior
     * executing this test.
     *
     * @param rootProjectPath the root project path of the genie-web module
     *
     * @return the path pointing to the build war
     */
    private static File resolveTestWar(String rootProjectPath) {

        File buildDir = new File(rootProjectPath, "/build/libs/");
        File[] war = buildDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile()
                        && pathname.getName().endsWith(".war");
            }
        });

        Assert.assertEquals("Unable to resolve war file for integration tests", 1, war.length);
        return war[0];
    }
}
